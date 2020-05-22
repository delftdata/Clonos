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

package org.apache.flink.runtime.causal.log;

import org.apache.flink.runtime.causal.VertexId;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ReplicatedCausalLog implements UpstreamCausalLog {


	private final VertexId vertexId;
	SortedMap<Long, Epoch> checkpointIDToEpoch;
	ConcurrentMap<InputChannelID, ConsumerIndex> consumerIDToOffset;


	public ReplicatedCausalLog(VertexId vertexId) {
		this.vertexId = vertexId;
		checkpointIDToEpoch = new TreeMap<>();
		checkpointIDToEpoch.put(0L, new Epoch(0L));
		consumerIDToOffset = new ConcurrentHashMap<>();
	}

	@Override
	public void registerDownstreamConsumer(InputChannelID inputChannelID) {
		consumerIDToOffset.put(inputChannelID, new ConsumerIndex(checkpointIDToEpoch.get(checkpointIDToEpoch.firstKey())));
	}

	@Override
	public ByteBuf getDeterminants() {
		CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
		for (Epoch epoch: checkpointIDToEpoch.values()) {
			compositeByteBuf.addComponent(epoch.getDeterminants());
		}
		return compositeByteBuf;
	}

	@Override
	public void processUpstreamVertexCausalLogDelta(CausalLogDelta causalLogDelta, long checkpointID) {
		Epoch epoch = checkpointIDToEpoch.get(checkpointID);
		//todo change to a nonblocking strategy
		synchronized (epoch){
			epoch.addSegment(causalLogDelta.rawDeterminants, causalLogDelta.offsetFromEpoch);
		}
	}

	@Override
	public CausalLogDelta getNextDeterminantsForDownstream(InputChannelID consumer, long checkpointID) {
		ConsumerIndex consumerIndex = consumerIDToOffset.get(consumer);
		if(consumerIndex.epoch.checkpointID != checkpointID)
			updateConsumerIndex(consumerIndex, checkpointID);

		Epoch epoch = checkpointIDToEpoch.get(checkpointID);
		int consumerOffset = consumerIDToOffset.get(consumer).getSegmentOffset();

		ByteBuf buf;
		//todo change to a nonblocking strategy
		synchronized (epoch){
			buf = epoch.getNextDeterminantsForDownstream(consumerOffset);
		}

		consumerIndex.setSegmentOffset(consumerOffset + buf.readableBytes());

		return new CausalLogDelta(vertexId, buf,consumerOffset);

	}

	private void updateConsumerIndex(ConsumerIndex consumerIndex, long checkpointID) {
		if(consumerIndex.epoch.checkpointID != checkpointID+1)
			throw new RuntimeException("Consumer skipped an epoch.");
		Epoch nextEpoch = checkpointIDToEpoch.computeIfAbsent(checkpointID+1, Epoch::new);
		consumerIndex.setEpoch(nextEpoch);
		consumerIndex.setSegmentOffset(0);
	}

	@Override
	public void unregisterDownstreamConsumer(InputChannelID toCancel) {
		consumerIDToOffset.remove(toCancel);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		//todo, may need atomic reference, that is if for a moment the value of checkpointIDToEpoch is undefined.
		checkpointIDToEpoch = checkpointIDToEpoch.tailMap(checkpointId);
	}

	//Epoch 1 is started by the barrier with checkpointId 1.
	//The buffer corresponding to that barrier is in epoch 0 however.
	@NotThreadSafe
	private static class Epoch {
		private long checkpointID;
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
				this.segments.add(segment.retainedSlice(newBytesStartOffset, segment.writerIndex()));
				currentOffsetFromEpochStart += numNewBytes;
			}
			//release once, as we no longer need the segment in its entirety.
			segment.release();
		}

		public ByteBuf getDeterminants() {
			//todo investigate the transfer of reference count ownership.
			return Unpooled.wrappedBuffer(segments.toArray(new ByteBuf[]{}));
		}

		public ByteBuf getNextDeterminantsForDownstream(int consumerOffset) {
			return Unpooled.wrappedBuffer(segments.subList(consumerOffset, segments.size()).toArray(new ByteBuf[]{}));
		}
	}

	private static class ConsumerIndex {
		Epoch epoch;
		//The offset in the epochs segment list. Points to the first not read element
		private int segmentOffset;

		public ConsumerIndex(Epoch epoch){
			this.epoch = epoch;
			this.segmentOffset = 0;
		}

		public Epoch getEpoch() {
			return epoch;
		}

		public void setEpoch(Epoch epoch) {
			this.epoch = epoch;
		}

		public int getSegmentOffset() {
			return segmentOffset;
		}

		public void setSegmentOffset(int segmentOffset) {
			this.segmentOffset = segmentOffset;
		}

	}
}
