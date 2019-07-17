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

import org.apache.flink.runtime.plugable.SerializationDelegate;

import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InFlightLogger<T, REC> {

	private static final Logger LOG = LoggerFactory.getLogger(InFlightLogger.class);

	private SortedMap<String, StreamSlice<REC>[]> slicedLog;

	private SerializationDelegate<REC> serializationDelegate;

	private int numOutgoingChannels;

	private boolean replaying = false;

	// Best effort id of current checkpoint.
	// It will always work for running tasks.
	// It should work for stateful standby tasks that receive state snapshots after each checkpoint.
	// It will be off for stateless standby tasks until they start running and receive the first checkpoint barrier.
	private String currentCheckpointId;


	public InFlightLogger(int numChannels) {
		slicedLog = new TreeMap<>();
		this.numOutgoingChannels = numChannels;
		this.currentCheckpointId = "0";
		this.serializationDelegate = null;
	}

	private void init(T serializedRecord) throws IOException {
		if (!(serializedRecord instanceof SerializationDelegate)) {
			throw new IOException("Record is not serialized within SerializationDelegate. It can not be logged to InFlightLogger.");
		}
		serializationDelegate = new SerializationDelegate<REC>((SerializationDelegate) serializedRecord);
		LOG.debug("Construct new serialization delegate {} from {}.", serializationDelegate, serializedRecord);

		// At the start of new epoch checkpoint id is unknown until triggerCheckpoint().
		// Only for the first checkpoint ever, create the slices here.
		// In case of standby tasks, the current checkpoint will not be the first.
		createSlices(currentCheckpointId);
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

	public void createSlices(String previousCheckpointId) {
		currentCheckpointId = String.valueOf(Long.valueOf(previousCheckpointId) + 1);

		StreamSlice<REC>[] newSlices = new StreamSlice[this.numOutgoingChannels];

		for(int i = 0; i < this.numOutgoingChannels; i++) {
			newSlices[i] = new StreamSlice<>(currentCheckpointId);
		}

		// Assumption: The checkpointId is a monotonically increasing long number starting from 1.
		slicedLog.put(currentCheckpointId, newSlices);
		LOG.debug("Create {} slices for checkpoint no {}.", numOutgoingChannels, currentCheckpointId);
	}

	public Iterable<T> getReplayLog(int outgoingChannelIndex) throws Exception {

		List<Iterator<REC>> wrappedIterators = new ArrayList<>(slicedLog.keySet().size());

		int totalRecordsToReplay = 0;

		for(String checkpointId : slicedLog.keySet()) {
			StreamSlice<REC> sliceOfChannel = slicedLog.get(checkpointId)[outgoingChannelIndex];
			wrappedIterators.add(sliceOfChannel.getSliceRecords().iterator());
			int recordsToReplay = sliceOfChannel.getSliceRecords().size();
			LOG.debug("For checkpoint id {} and channel index {}, {} records have been logged.", checkpointId, outgoingChannelIndex, recordsToReplay);
			totalRecordsToReplay += recordsToReplay;
		}

		if (wrappedIterators.size() == 0) {
			return new Iterable<T>() {
				@Override
				public Iterator<T> iterator() {
					return Collections.emptyListIterator();
				}
			};
		}

		LOG.debug("Get in-flight log of channel {} to replay {} records.", outgoingChannelIndex, totalRecordsToReplay);

		replaying = true;
		LOG.debug("Start replay log. Set replaying to true.");

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
							LOG.debug("End of replay log. Set replaying to false.");
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

	// Standby tasks receive a state snapshot and the checkpoint id upon each checkpoint.
	public void updateCheckpointId(String checkpointId) {
		currentCheckpointId = checkpointId;
		LOG.debug("Updated checkpointId of {}.", this);
	}

	public void clearLog() throws Exception {
		LOG.debug("Clear the in-flight log.");
		slicedLog = new TreeMap<>();
	}

	public void discardSlice() {
		LOG.debug("Discard slices for checkpoint {}.", slicedLog.firstKey());
		slicedLog.remove(slicedLog.firstKey());
	}

	public void discardSlice(String checkpointId) {
		LOG.debug("Discard slices for checkpoint {}.", checkpointId);
		slicedLog.remove(checkpointId);

		// Also discard the in-flight log of previous checkpoints that never completed.
		while (Long.valueOf(slicedLog.firstKey()) < Long.valueOf(checkpointId)) {
			LOG.debug("Discard slices for checkpoint {}.", slicedLog.firstKey());
			slicedLog.remove(slicedLog.firstKey());
		}
	}

	public String toString() {
		return String.format("InFlightLogger [replaying: %s, channels: %s, current checkpoint id: %s]", replaying, numOutgoingChannels, currentCheckpointId);
	}

}
