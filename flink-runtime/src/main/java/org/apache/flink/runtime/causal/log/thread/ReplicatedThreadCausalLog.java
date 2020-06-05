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
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled.EMPTY_BUFFER;

/**
 * This is a replicatedThreadCausalLog, thus it is a MPMC log
 */
public class ReplicatedThreadCausalLog implements UpstreamThreadCausalLog {



	SortedMap<Long, Epoch> checkpointIDToEpoch;
	ConcurrentMap<InputChannelID, ConsumerIndex> consumerIDToOffset;

	public ReplicatedThreadCausalLog() {
		checkpointIDToEpoch = new TreeMap<>();
		consumerIDToOffset = new ConcurrentHashMap<>();
	}

	@Override
	public ByteBuf getDeterminants() {
		return Unpooled.wrappedBuffer(checkpointIDToEpoch.values().stream().map(Epoch::getDeterminants).toArray(ByteBuf[]::new));
	}

	@Override
	public void processUpstreamVertexCausalLogDelta(ThreadLogDelta causalLogDelta, long epochID) {
		Epoch epoch = checkpointIDToEpoch.computeIfAbsent(epochID, Epoch::new);
		//todo change to a nonblocking strategy
		synchronized (epoch){
			epoch.addSegment(causalLogDelta.rawDeterminants, causalLogDelta.offsetFromEpoch);
		}
	}

	@Override
	public ThreadLogDelta getNextDeterminantsForDownstream(InputChannelID consumer, long checkpointID) {
		ConsumerIndex consumerIndex = consumerIDToOffset.computeIfAbsent(consumer, k -> new ConsumerIndex(checkpointID));

		if(consumerIndex.getEpochID() != checkpointID)
			consumerIndex.startConsumingEpoch(checkpointID);

		int consumerSegmentOffset = consumerIndex.getSegmentOffset();
		int consumerByteOffset = consumerIndex.getByteOffset();

		Epoch requestedEpoch = checkpointIDToEpoch.computeIfAbsent(checkpointID, Epoch::new);

		BufferAndSegmentOffset bufferAndSegmentOffset;
		synchronized (requestedEpoch){
			bufferAndSegmentOffset = requestedEpoch.getNextDeterminantsForDownstream(consumerSegmentOffset);
		}

		consumerIndex.setSegmentOffset(bufferAndSegmentOffset.offset);
		consumerIndex.setByteOffset(consumerByteOffset + bufferAndSegmentOffset.getBuf().readableBytes());

		return new ThreadLogDelta(bufferAndSegmentOffset.getBuf(), consumerByteOffset);

	}


	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		List<Long> toRemove = new LinkedList<>();
		for(Long epoch : checkpointIDToEpoch.keySet())
			if(epoch < checkpointId)
				toRemove.add(epoch);

		for(Long epoch : toRemove)
			checkpointIDToEpoch.remove(epoch).segments.forEach(ByteBuf::release);

	}

	//Epoch 1 is started by the barrier with checkpointId 1.
	//The buffer corresponding to that barrier is in epoch 0 however.
	@NotThreadSafe
	private static class Epoch {
		private long checkpointID;

		@Override
		public String toString() {
			return "Epoch{" +
				"checkpointID=" + checkpointID +
				", segments=" + "["+segments.stream().map(Object::toString).collect(Collectors.joining(",")) +"]"+
				", currentOffsetFromEpochStart=" + currentOffsetFromEpochStart +
				'}';
		}

		private List<ByteBuf> segments;
		private int currentOffsetFromEpochStart;

		public Epoch(long checkpointID) {
			this.checkpointID = checkpointID;
			this.segments = new LinkedList<>();
			this.currentOffsetFromEpochStart = 0;
		}

		public void addSegment(ByteBuf segment, int segmentOffsetFromEpochStart){
			int numNewBytes = segmentOffsetFromEpochStart + segment.readableBytes() - currentOffsetFromEpochStart;

			if(numNewBytes > 0){
				int newBytesStartOffset = segment.readableBytes() - numNewBytes;
				this.segments.add(segment.retainedSlice(segment.readerIndex() + newBytesStartOffset, numNewBytes));
				currentOffsetFromEpochStart += numNewBytes;
			}
			//release once, as we no longer need the segment in its entirety.
			segment.release();
		}

		public ByteBuf getDeterminants() {
			return Unpooled.unmodifiableBuffer(segments.toArray(new ByteBuf[segments.size()])).retain();
		}

		public BufferAndSegmentOffset getNextDeterminantsForDownstream(int consumerOffset) {
			System.out.println("Consumer Offset: " + consumerOffset);
			ByteBuf buffer = (consumerOffset < segments.size() ? Unpooled.unmodifiableBuffer(segments.subList(consumerOffset, segments.size()).toArray(new ByteBuf[segments.size() - consumerOffset])).retain() : EMPTY_BUFFER);
			return new BufferAndSegmentOffset(
				buffer,
				segments.size());
		}

		public long getCheckpointID() {
			return checkpointID;
		}
	}

	private static class ConsumerIndex {
		long epochID;
		//The offset in the epochs segment list. Points to the first not read element
		private int segmentOffset;

		private int byteOffset;

		public ConsumerIndex(long epochID){
			this.epochID = epochID;
			this.segmentOffset = 0;
			this.byteOffset = 0;
		}

		public long getEpochID() {
			return epochID;
		}

		public void startConsumingEpoch(long epochID) {
			this.epochID = epochID;
			segmentOffset = 0;
			byteOffset = 0;
		}

		public int getSegmentOffset() {
			return segmentOffset;
		}

		public void setSegmentOffset(int segmentOffset) {
			this.segmentOffset = segmentOffset;
		}

		public int getByteOffset() {
			return byteOffset;
		}

		public void setByteOffset(int byteOffset) {
			this.byteOffset = byteOffset;
		}

		@Override
		public String toString() {
			return "ConsumerIndex{" +
				"epochID=" + epochID +
				", segmentOffset=" + segmentOffset +
				", byteOffset=" + byteOffset +
				'}';
		}

	}

	private static class BufferAndSegmentOffset {
		private ByteBuf buf;
		private int offset;

		public BufferAndSegmentOffset(ByteBuf buf, int offset){
			this.buf = buf;
			this.offset = offset;
		}

		public ByteBuf getBuf() {
			return buf;
		}

		public int getOffset() {
			return offset;
		}
	}
}
