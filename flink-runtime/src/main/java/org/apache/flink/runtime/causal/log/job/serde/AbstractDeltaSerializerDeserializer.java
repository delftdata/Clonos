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

package org.apache.flink.runtime.causal.log.job.serde;

import org.apache.flink.runtime.causal.log.job.CausalLogID;
import org.apache.flink.runtime.causal.log.thread.ThreadCausalLog;
import org.apache.flink.runtime.causal.log.thread.ThreadCausalLogImpl;
import org.apache.flink.runtime.causal.recovery.AbstractState;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public abstract class AbstractDeltaSerializerDeserializer implements DeltaSerializerDeserializer {
	protected final ConcurrentSkipListMap<CausalLogID, ThreadCausalLog> threadCausalLogs;
	protected final BufferPool determinantBufferPool;

	protected final Map<InputChannelID, CausalLogID> outputChannelSpecificCausalLogs;

	protected final short localVertexID;

	protected final Map<Short, Integer> vertexIDToDistance;

	protected final int determinantSharingDepth;


	protected static final Logger LOG = LoggerFactory.getLogger(AbstractDeltaSerializerDeserializer.class);

	public AbstractDeltaSerializerDeserializer(ConcurrentSkipListMap<CausalLogID, ThreadCausalLog> threadCausalLogs,
											   Map<Short, Integer> vertexIDToDistance, int determinantSharingDepth,
											   short localVertexID, BufferPool determinantBufferPool) {
		this.threadCausalLogs = threadCausalLogs;
		this.outputChannelSpecificCausalLogs = new HashMap<>();
		this.determinantBufferPool = determinantBufferPool;
		this.vertexIDToDistance = vertexIDToDistance;
		this.determinantSharingDepth = determinantSharingDepth;
		this.localVertexID = localVertexID;
	}


	@Override
	public ByteBuf enrichWithCausalLogDelta(ByteBuf serialized, InputChannelID outputChannelID, long epochID) {
		ByteBufAllocator allocator = serialized.alloc();
		CompositeByteBuf composite = allocator.compositeDirectBuffer(Integer.MAX_VALUE);

		ByteBuf deltaHeader = allocator.directBuffer();
		deltaHeader.writeInt(0);//Header Size
		deltaHeader.writeLong(epochID);//Epoch


		serializeDataStrategy(outputChannelID, epochID, composite, deltaHeader);

		deltaHeader.setInt(0, deltaHeader.readableBytes());

		LOG.info("enrichWithCausalLogDelta: headerBytes: {}, epochID: {}", deltaHeader.readableBytes(), epochID);

		int addedSize = composite.readableBytes() + deltaHeader.readableBytes();

		composite.addComponent(true, 0, serialized);
		composite.addComponent(true, 1, deltaHeader);

		composite.setInt(0, composite.getInt(0) + addedSize);

		return composite;
	}

	@Override
	public void processCausalLogDelta(ByteBuf msg) {

		CausalLogID causalLogID = new CausalLogID();

		int headerBytesStart = msg.readerIndex();
		int headerBytes = msg.readInt();
		int headerBytesEnd = headerBytesStart + headerBytes; //Where the header ends and the deltas start
		int deltaIndex = headerBytesEnd;
		long epochID = msg.readLong();

		LOG.info("processCausalLogDelta: headerBytes: {}, epochID: {}", headerBytes, epochID);

		while (msg.readerIndex() < headerBytesEnd) {
			deltaIndex = deserializeStrategyStep(msg, causalLogID, deltaIndex, epochID);
		}
	}

	protected int processThreadDelta(ByteBuf msg, CausalLogID causalLogID, int deltaIndex, long epochID) {

		if (!threadCausalLogs.containsKey(causalLogID)) {
			//If we that mapping is not present, we need to clone the key, so it is not mutated
			CausalLogID idToInsert = new CausalLogID(causalLogID);

			threadCausalLogs.computeIfAbsent(idToInsert, k -> new ThreadCausalLogImpl(determinantBufferPool));
		}

		ThreadCausalLog threadLog = threadCausalLogs.get(causalLogID);

		int offsetFromEpoch = msg.readInt();
		int bufferSize = msg.readInt();
		ByteBuf delta = msg.retainedSlice(deltaIndex, bufferSize);

		LOG.info("processThreadDelta: causalLogID: {}, offsetOfConsumer: {}, bufSize: {}", causalLogID, offsetFromEpoch, bufferSize);
		threadLog.processUpstreamDelta(delta, offsetFromEpoch, epochID);

		return bufferSize;
	}

	protected void serializeThreadDelta(InputChannelID outputChannelID, long epochID, CompositeByteBuf composite,
										ByteBuf deltaHeader, ThreadCausalLog log, CausalLogID causalLogID) {
		int offsetOfConsumer = log.getOffsetFromEpochForConsumer(outputChannelID, epochID);
		deltaHeader.writeInt(offsetOfConsumer);
		ByteBuf deltaBuf = log.getDeltaForConsumer(outputChannelID, epochID);
		deltaHeader.writeInt(deltaBuf.readableBytes());
		LOG.info("serializeDelta: causalLogID: {}, offsetOfConsumer: {}, bufSize: {}", causalLogID, offsetOfConsumer, deltaBuf.readableBytes());
		composite.addComponent(true, deltaBuf);
	}


	@Override
	public void registerDownstreamConsumer(InputChannelID outputChannelID, CausalLogID consumedCausalLog) {
		this.outputChannelSpecificCausalLogs.put(outputChannelID, consumedCausalLog);
	}

	protected abstract void serializeDataStrategy(InputChannelID outputChannelID, long epochID,
												  CompositeByteBuf composite, ByteBuf deltaHeader);

	protected abstract int deserializeStrategyStep(ByteBuf msg, CausalLogID causalLogID, int deltaIndex, long epochID);
}
