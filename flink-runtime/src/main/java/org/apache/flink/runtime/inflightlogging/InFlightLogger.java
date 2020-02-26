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
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.ResultPartition;

import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InFlightLogger implements BufferRecycler {

	private static final Logger LOG = LoggerFactory.getLogger(InFlightLogger.class);

	private SortedMap<Long, StreamSlice<BufferBuilder>[]> slicedLog;

	private final ArrayDeque<MemorySegment> memorySegmentQueue = new ArrayDeque<>();

	private int numTotalSegments;

	private final ResultPartition targetPartition;

	private int numOutgoingChannels;

	private boolean replaying = false;

	// Best effort id of current checkpoint.
	// It will always work for running tasks.
	// It should work for stateful standby tasks that receive state snapshots after each checkpoint.
	// It will be off for stateless standby tasks until they start running and receive the first checkpoint barrier.
	private long currentCheckpointId;

	public InFlightLogger(ResultPartitionWriter targetPartition, int numChannels) throws IOException {
		slicedLog = new TreeMap<>();
		this.numOutgoingChannels = numChannels;
		this.currentCheckpointId = 0;
		this.numTotalSegments = 10;

		if (targetPartition instanceof ResultPartition) {
			this.targetPartition = (ResultPartition) targetPartition;
			for (int i = 0; i < numOutgoingChannels; i++) {
				assignExclusiveSegments(this.targetPartition.assignExclusiveSegments(numTotalSegments));
			}
		} else {
			throw new IOException("Unable to operate the InFlightLogger. Partition is of type " + targetPartition + ".");
		}
	}

	public boolean replaying() {
		return replaying;
	}

	public void setReplaying(boolean newReplayingState) {
		replaying = newReplayingState;
	}

	public void log(BufferBuilder bufferBuilder, int channelIndex) throws InterruptedException {
		if (!replaying) {

			// At the start of new epoch checkpoint id is unknown until triggerCheckpoint(). Only for the first checkpoint ever, create the slices here. In case of standby tasks, the current checkpoint will not be the first.
			if (slicedLog.isEmpty()) {
				createSlices(currentCheckpointId);
			}

			StreamSlice currentSlice = slicedLog.get(slicedLog.lastKey())[channelIndex];

			LOG.debug("{}: Request memory segment.", this);
			MemorySegment segment = requestSegment();

			LOG.debug("{}: got {}. Log {}.", this, segment, bufferBuilder);
			BufferBuilder newBufferBuilder = new BufferBuilder(bufferBuilder, segment, this);
			newBufferBuilder.finish();
			currentSlice.addData(newBufferBuilder);
		}
	}

	public void createSlices(long previousCheckpointId) {
		currentCheckpointId = previousCheckpointId + 1;

		StreamSlice<BufferBuilder>[] newSlices = new StreamSlice[this.numOutgoingChannels];

		for (int i = 0; i < this.numOutgoingChannels; i++) {
			newSlices[i] = new StreamSlice<>(currentCheckpointId);
		}

		// Assumption: The checkpointId is a monotonically increasing long number starting from 1.
		slicedLog.put(currentCheckpointId, newSlices);
		LOG.info("Create {} slices for checkpoint no {}.", numOutgoingChannels, currentCheckpointId);
	}

	public boolean assignExclusiveSegments(List<MemorySegment> segments) {
		synchronized (memorySegmentQueue) {
			LOG.debug("Received {} segments from the pool.", segments.size());
			for (MemorySegment segment : segments) {
				memorySegmentQueue.add(segment);
			}
			LOG.debug("{} memorySegmentQueue currently has {} available buffers.", this, memorySegmentQueue.size());

			if (segments.isEmpty()) {
				return false;
			}
			return true;
		}
	}

	public MemorySegment requestSegment() throws InterruptedException {
		MemorySegment segment = null;
		synchronized (memorySegmentQueue) {
			while (segment == null) {
				segment = memorySegmentQueue.poll();
				if (segment != null) {
					break;
				}

				if (!assignExclusiveSegments(targetPartition.assignExclusiveSegments(numTotalSegments))) {
					LOG.info("{}: Wait 0.5 seconds for MemorySegment to become available.", this);
					memorySegmentQueue.wait(500);
					numTotalSegments = 10;
				} else {
					numTotalSegments *= 2;
				}
			}
		}
		return segment;
	}

	public void recycle(MemorySegment segment) {
		synchronized (memorySegmentQueue) {
			memorySegmentQueue.add(segment);
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

	public List<BufferBuilder> getReplayLog(int outgoingChannelIndex, long checkpointId) throws IOException {

		List<BufferBuilder> sliceOfChannel = slicedLog.get(checkpointId)[outgoingChannelIndex].getSliceData();
		LOG.info("{}: For checkpoint id {} and channel index {}, {} BufferBuilders have been logged.", this, checkpointId, outgoingChannelIndex, sliceOfChannel.size());

		replaying = true;
		LOG.debug("{}: Start replay log for checkpoint {}. Set replaying to true.", this, checkpointId);
		return sliceOfChannel;
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
			for (int channel = 0; channel < numOutgoingChannels; channel++) {
				List<BufferBuilder> bufferBuilders = slicedLog.get(checkpointId)[channel].getSliceData();
				LOG.debug("{}: Recycle {} bufferBuilders.", this, bufferBuilders.size());
				for (BufferBuilder bufferBuilder : bufferBuilders) {
					recycle(bufferBuilder.getMemorySegment());
				}
			}
			slicedLog.remove(checkpointId);
		} else {
			LOG.warn("Abort discard slices: no slices or checkpoint barrier for checkpoint {}.", checkpointId);
		}

		// Also discard the in-flight log of previous checkpoints that never completed.
		while (!slicedLog.isEmpty() && slicedLog.firstKey() < checkpointId) {
			LOG.info("Discard slices for checkpoint {}.", slicedLog.firstKey());
			for (int channel = 0; channel < numOutgoingChannels; channel++) {
				List<BufferBuilder> bufferBuilders = slicedLog.get(slicedLog.firstKey())[channel].getSliceData();
				LOG.debug("{}: Recycle {} bufferBuilders.", this, bufferBuilders.size());
				for (BufferBuilder bufferBuilder : bufferBuilders) {
					recycle(bufferBuilder.getMemorySegment());
				}
			}
			slicedLog.remove(slicedLog.firstKey());
		}
	}

	public String toString() {
		int segments;
		synchronized (memorySegmentQueue) {
			segments = memorySegmentQueue.size();
		}

		return String.format("InFlightLogger [task: %s, replaying: %s, channels: %s, current checkpoint id: %d, totalSegments: %d, available segments: %d]", targetPartition.getTaskName(), replaying, numOutgoingChannels, currentCheckpointId, numTotalSegments, segments);
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
