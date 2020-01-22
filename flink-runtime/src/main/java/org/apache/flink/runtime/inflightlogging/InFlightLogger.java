/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.inflightlogging;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InFlightLogger<T, REC> implements BufferRecycler {

	private static final Logger LOG = LoggerFactory.getLogger(InFlightLogger.class);

	private SortedMap<Long, StreamSlice<REC>[]> slicedLog;

	private SortedMap<Long, StreamSlice<Buffer>[]> slicedBufferLog;

	private final ArrayDeque<Buffer> bufferQueue = new ArrayDeque<>();

	private SerializationDelegate<REC> serializationDelegate;

	private int numOutgoingChannels;

	private boolean replaying = false;

	// Best effort id of current checkpoint.
	// It will always work for running tasks.
	// It should work for stateful standby tasks that receive state snapshots after each checkpoint.
	// It will be off for stateless standby tasks until they start running and receive the first checkpoint barrier.
	private long currentCheckpointId;

	public InFlightLogger(int numChannels) {
		slicedLog = new TreeMap<>();
		slicedBufferLog = new TreeMap<>();
		this.numOutgoingChannels = numChannels;
		this.currentCheckpointId = 0;
		this.serializationDelegate = null;
	}

	private void init(T serializedRecord) throws IOException {
		if (!(serializedRecord instanceof SerializationDelegate)) {
			throw new IOException("Record is not serialized within SerializationDelegate. It can not be logged to InFlightLogger.");
		}
		boolean copyConstructor = true;
		serializationDelegate = new SerializationDelegate<REC>((SerializationDelegate) serializedRecord, copyConstructor);
		LOG.debug("Construct new serialization delegate {} from {}.", serializationDelegate, serializedRecord);

		// At the start of new epoch checkpoint id is unknown until triggerCheckpoint().
		// Only for the first checkpoint ever, create the slices here.
		// In case of standby tasks, the current checkpoint will not be the first.
		if (slicedLog.isEmpty()) {
			createSlices(currentCheckpointId);
		}
	}

	public boolean replaying() {
		return replaying;
	}

	public void logRecord(T serializedRecord, int channelIndex) throws IOException {
		if (serializationDelegate == null) {
			init(serializedRecord);
		}

		if (!replaying) {
			StreamSlice currentSlice = slicedLog.get(slicedLog.lastKey())[channelIndex];
			currentSlice.addRecord((REC) ((SerializationDelegate) serializedRecord).copyInstance());
		}
	}

	public void createSlices(long previousCheckpointId) {
		currentCheckpointId = previousCheckpointId + 1;

		StreamSlice<REC>[] newSlices = new StreamSlice[this.numOutgoingChannels];
		StreamSlice<Buffer>[] newBufferSlices = new StreamSlice[this.numOutgoingChannels];

		for (int i = 0; i < this.numOutgoingChannels; i++) {
			newSlices[i] = new StreamSlice<>(currentCheckpointId);
			newBufferSlices[i] = new StreamSlice<>(currentCheckpointId);
		}

		// Assumption: The checkpointId is a monotonically increasing long number starting from 1.
		slicedLog.put(currentCheckpointId, newSlices);
		slicedBufferLog.put(currentCheckpointId, newBufferSlices);
		LOG.info("Create {} slices for checkpoint no {}.", numOutgoingChannels, currentCheckpointId);
	}

	public void assignExclusiveSegments(List<MemorySegment> segments) {
		synchronized (bufferQueue) {
			for (MemorySegment segment : segments) {
				bufferQueue.add(new NetworkBuffer(segment, this));
			}
			LOG.info("InFlightLogger bufferQueue currently has {} available buffers.", bufferQueue.size());
		}
	}

	public void recycle(MemorySegment segment) {
		synchronized (bufferQueue) {
			bufferQueue.add(new NetworkBuffer(segment, this));
		}
	}

	public TreeSet<Long> getCheckpointIdsToReplay(long downstreamCheckpointId) {
		TreeSet<Long> checkpointIdsToReplay = new TreeSet<>();
		for (long checkpointId : slicedLog.keySet()) {
			if (checkpointId <= downstreamCheckpointId) {
				LOG.info("Skip replaying of records for checkpoint {}. Downstream has snapshot of checkpoint {}.", checkpointId, downstreamCheckpointId);
				continue;
			}
			checkpointIdsToReplay.add(checkpointId);
		}
		return checkpointIdsToReplay;
	}

	public Iterable<T> getReplayLog(int outgoingChannelIndex, long checkpointId) throws IOException {

		// List is not required for single checkpoint.
		// Code can be simplified.
		List<Iterator<REC>> wrappedIterators = new ArrayList<>(slicedLog.keySet().size());

		StreamSlice<REC> sliceOfChannel = slicedLog.get(checkpointId)[outgoingChannelIndex];
		wrappedIterators.add(sliceOfChannel.getSliceRecords().iterator());
		int recordsToReplay = sliceOfChannel.getSliceRecords().size();
		LOG.info("For checkpoint id {} and channel index {}, {} records have been logged.", checkpointId, outgoingChannelIndex, recordsToReplay);

		if (wrappedIterators.size() == 0) {
			return new Iterable<T>() {
				@Override
				public Iterator<T> iterator() {
					return Collections.emptyListIterator();
				}
			};
		}

		LOG.debug("Get in-flight log of channel {} to replay {} records.", outgoingChannelIndex, recordsToReplay);

		replaying = true;
		LOG.debug("Start replay log for checkpoint {}. Set replaying to true.", checkpointId);

		return new Iterable<T>() {
			@Override
			public Iterator<T> iterator() {

				return new Iterator<T>() {
					int indx = 0;
					Iterator<REC> currentIterator = wrappedIterators.get(0);

					@Override
					public boolean hasNext() {
						if (!currentIterator.hasNext()) {
							progressLog();
						}
						return currentIterator.hasNext();
					}

					@Override
					public T next() {
						if (!currentIterator.hasNext() && indx < wrappedIterators.size()) {
							progressLog();
						}
						serializationDelegate.setInstance((REC) currentIterator.next());
						return (T) serializationDelegate;
					}

					private void progressLog() {
						while (!currentIterator.hasNext() && ++indx < wrappedIterators.size()) {
							currentIterator = wrappedIterators.get(indx);
						}
						if (!currentIterator.hasNext() && indx == wrappedIterators.size()) {
							LOG.debug("End of replay log for checkpoint {}. Set replaying to false.", checkpointId);
							replaying = false;
						}
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}

				};
			}
		};
	}

	public long getCheckpointId() {
		return currentCheckpointId;
	}

	// Standby tasks receive a state snapshot and the checkpoint id upon each checkpoint.
	public void updateCheckpointId(long checkpointId) {
		currentCheckpointId = checkpointId;
		LOG.info("Updated checkpointId of {}.", this);
	}

	public void clearLog() throws Exception {
		LOG.debug("Clear the in-flight log.");
		slicedLog = new TreeMap<>();
	}

	public void discardSlice(long checkpointId) {
		LOG.info("Discard slices for checkpoint {}.", checkpointId);
		if (slicedLog.get(checkpointId) != null && slicedLog.get(checkpointId)[0].getCheckpointBarrier() != null) {
			slicedLog.remove(checkpointId);
		} else {
			LOG.warn("Abort discard slices: no slices or checkpoint barrier for checkpoint {}.", checkpointId);
		}

		// Also discard the in-flight log of previous checkpoints that never completed.
		while (!slicedLog.isEmpty() && slicedLog.firstKey() < checkpointId) {
			LOG.info("Discard slices for checkpoint {}.", slicedLog.firstKey());
			slicedLog.remove(slicedLog.firstKey());
		}
	}

	public String toString() {
		return String.format("InFlightLogger [replaying: %s, channels: %s, current checkpoint id: %d]", replaying, numOutgoingChannels, currentCheckpointId);
	}

	public void logCheckpointBarrier(CheckpointBarrier checkpointBarrier) {
		try {
			LOG.info("Log {}.", checkpointBarrier);
			for (int channelIndex = 0; channelIndex < this.numOutgoingChannels; channelIndex++) {
				slicedLog.get(checkpointBarrier.getId())[channelIndex].setCheckpointBarrier(checkpointBarrier);
			}
		} catch (NullPointerException e) {
			LOG.warn("No in-flight log to store {}. This means that no records appeared in that epoch. Create the missing slices and store it.", checkpointBarrier);
			createSlices(checkpointBarrier.getId() - 1);
			logCheckpointBarrier(checkpointBarrier);
		}
	}

	public CheckpointBarrier getCheckpointBarrier(int channelIndex, long checkpointId) {
		try {
			return slicedLog.get(checkpointId)[channelIndex].getCheckpointBarrier();
		} catch (NullPointerException e) {
			LOG.warn("No in-flight log to get CheckpointBarrier for channel {} and checkpoint {}.", channelIndex, checkpointId);
			return null;
		}
	}
}
