/*
 *
 *
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 *
 *
 */

package org.apache.flink.runtime.causal.log.thread;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


/**
 * Implements the {@link ThreadCausalLog} as a Growable Circular Array with indexing for checkpoints and consumers.
 * This implementation should be used for logging the local vertex.
 */
public class CircularLocalThreadLogThread implements LocalThreadCausalLog {

	private static final Logger LOG = LoggerFactory.getLogger(CircularLocalThreadLogThread.class);
	private static final int DEFAULT_START_SIZE = 65536;
	private static final int GROWTH_FACTOR = 2;

	private byte[] array;
	//todo change to atomics
	//The first position of the array which has data
	private int start;
	//The first position of the array that does NOT have data
	private int end;

	private int size;

	private List<EpochStartOffset> epochStartOffsets;
	private Map<InputChannelID, DownstreamChannelOffset> channelOffsetMap;


	public CircularLocalThreadLogThread() {
		this(DEFAULT_START_SIZE);
	}

	public CircularLocalThreadLogThread(int startSize) {
		array = new byte[startSize];
		epochStartOffsets = new LinkedList<>();
		start = 0;
		end = 0;
		size = 0;

		EpochStartOffset startEpochOffset = new EpochStartOffset(0l, 0);
		epochStartOffsets.add(startEpochOffset);

		//initialized to 0s
		channelOffsetMap = new HashMap<>();
	}

	//@Override
	//public synchronized void registerDownstreamConsumer(InputChannelID inputChannelID) {
	//	channelOffsetMap.put(inputChannelID, new DownstreamChannelOffset(getEarliestEpochOffset()));
	//}


	@Override
	public synchronized ByteBuf getDeterminants(long startEpochID) {
		byte[] copy = new byte[size];
		circularArrayCopyOutOfLog(array, start, end - start, copy);
		return Unpooled.wrappedBuffer(copy);
	}

	@Override
	public synchronized void appendDeterminants(byte[] determinants, long epochID) {
		while (!hasSpaceFor(determinants.length))
			grow();

		circularCopyIntoLog(determinants, 0);
	}

	//@Override
	//public synchronized void unregisterDownstreamConsumer(InputChannelID toCancel) {
	//	this.channelOffsetMap.remove(toCancel);
	//}

	@Override
	public synchronized void notifyCheckpointComplete(long checkpointId) throws Exception {
		EpochStartOffset top;
		while (true) {
			top = epochStartOffsets.get(0);

			size -= circularDistance(start, top.offset, array.length);
			start = top.offset; //delete everything pertaining to this epoch

			if (checkpointId > top.id) // Keep the offset of the checkpoint that was just completed.
				epochStartOffsets.remove(0);
			else
				break;
		}
	}

	@Override
	public synchronized ThreadLogDelta getNextDeterminantsForDownstream(InputChannelID channel, long epochID) {


		DownstreamChannelOffset channelOffset = channelOffsetMap.get(channel);

		//Calculate the physical offset in the array that the downstream is at.
		int physicalOffsetForDownstream = channelOffset.epochStart.offset + channelOffset.offset;

		//Save a copy of the logical start offset of the downstream
		int logicalStartOffset = channelOffset.offset;

		//Given that we now send determinants in netty, we should send only
		// the minimum between the current amount of determinants and the start of the next epoch.
		int sendUpTo = (channelOffset.epochStart.getId() == getLatestEpochOffset().id ? end : channelOffset.epochStart.next.offset);

		int numberOfBytesToCopy = circularDistance(physicalOffsetForDownstream, sendUpTo, array.length);
		byte[] toReturn = new byte[numberOfBytesToCopy];

		circularArrayCopyOutOfLog(array, physicalOffsetForDownstream, numberOfBytesToCopy, toReturn);

		if(sendUpTo == end)
			channelOffset.setOffset(circularDistance(getLatestEpochOffset().offset, end, array.length)); //channel has all the latest determinants
		else {
			channelOffset.setEpochStart(channelOffset.epochStart.next);
			channelOffset.setOffset(0);
		}

		return new ThreadLogDelta(Unpooled.wrappedBuffer(toReturn), logicalStartOffset);
	}

	@Override
	public int logLength() {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public String toString() {
		return "CircularVertexCausalLog{" +
			", start=" + start +
			", end=" + end +
			", size=" + size +
			", epochStartOffsets=[" + epochStartOffsets.stream().map(Objects::toString).collect(Collectors.toList()) +"]"+
			", channelOffsets=[" + channelOffsetMap.entrySet().stream().map(e -> e.getKey().toString() + " -> " + e.getValue().toString()).collect(Collectors.joining(", ")) + "]" +
			'}';
	}

	private EpochStartOffset getLatestEpochOffset() {
		return epochStartOffsets.get(epochStartOffsets.size() - 1);
	}

	private EpochStartOffset getEarliestEpochOffset() {
		return epochStartOffsets.get(0);
	}

	/**
	 * @param from      start point (inclusive)
	 * @param to        end point (exclusive)
	 * @param totalSize
	 * @return
	 */
	private static int circularDistance(int from, int to, int totalSize) {
		return Math.floorMod((to - from), totalSize);
	}

	private void circularArrayCopyOutOfLog(byte[] from, int start, int numBytesToCopy, byte[] to) {
		if (start + numBytesToCopy <= from.length) {
			System.arraycopy(from, start, to, 0, numBytesToCopy);
		} else { //Goes around the circle
			int numBytesToCopyBeforeCompleteCircle = from.length - start;
			System.arraycopy(from, start, to, 0, numBytesToCopyBeforeCompleteCircle);
			System.arraycopy(from, start, to, numBytesToCopyBeforeCompleteCircle, numBytesToCopy - numBytesToCopyBeforeCompleteCircle);
		}
	}

	/**
	 * Grows the array and moves everything to position 0
	 */
	private void grow() {
		byte[] newArray = new byte[array.length * GROWTH_FACTOR];

		circularArrayCopyOutOfLog(array, start, size, newArray);
		int move = -start;
		start = 0;
		end = size;

		// Since we reset to 0, we need to move the offsets as well.
		for (EpochStartOffset offset : epochStartOffsets) {
			offset.setOffset((offset.offset + move) % array.length);
		}

		array = newArray;
	}

	/**
	 * PRECONDITION: has enough space!
	 *
	 * @param determinants
	 * @param offset
	 */
	private void circularCopyIntoLog(byte[] determinants, int offset) {
		int numDeterminantsToCopy = determinants.length - offset;
		int bytesUntilLoop = circularDistance(end, array.length, array.length);
		LOG.info("NumDeterminantsToCopy={}, bytesUntilLoop={}", numDeterminantsToCopy, bytesUntilLoop);
		if (numDeterminantsToCopy > bytesUntilLoop) {
			System.arraycopy(determinants, offset, array, end, bytesUntilLoop);
			System.arraycopy(determinants, offset + bytesUntilLoop, array, 0, numDeterminantsToCopy - bytesUntilLoop);
		} else {
			System.arraycopy(determinants, offset, array, end, numDeterminantsToCopy);
		}
		end = (end + numDeterminantsToCopy) % array.length;
		size += numDeterminantsToCopy;
	}

	private boolean hasSpaceFor(int toAdd) {
		return array.length - size >= toAdd;
	}

	private class EpochStartOffset {
		//The checkpoint id that initiates this epoch
		private long id;
		//The physical offset in the circular log of the first element after the checkpoint
		private int offset;

		private EpochStartOffset next;

		public EpochStartOffset(long id, int offset) {
			this.id = id;
			this.offset = offset;
			this.next = null;
		}

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public int getOffset() {
			return offset;
		}

		public void setOffset(int offset) {
			this.offset = offset;
		}

		public EpochStartOffset getNext() {
			return next;
		}

		public void setNext(EpochStartOffset next) {
			this.next = next;
		}

		@Override
		public String toString() {
			return "CheckpointOffset{" +
				"id=" + id +
				", offset=" + offset +
				", next=" + (next == null ? "none" : next.id) +
				'}';
		}
	}

	/**
	 * Marks the next element to be read by the downstream consumer
	 */
	private class DownstreamChannelOffset {
		// Refers to the epoch that the downstream is currently in
		private EpochStartOffset epochStart;

		// The logical offset from that epoch
		private int offset;

		public DownstreamChannelOffset(EpochStartOffset epochStart) {
			this.epochStart = epochStart;
			this.offset = 0;
		}

		public EpochStartOffset getEpochStart() {
			return epochStart;
		}

		public void setEpochStart(EpochStartOffset epochStart) {
			this.epochStart = epochStart;
		}

		public int getOffset() {
			return offset;
		}

		public void setOffset(int offset) {
			this.offset = offset;
		}

		@Override
		public String toString() {
			return "DownstreamChannelOffset{" +
				"epochStart=" + epochStart +
				", offset=" + offset +
				'}';
		}
	}

}

