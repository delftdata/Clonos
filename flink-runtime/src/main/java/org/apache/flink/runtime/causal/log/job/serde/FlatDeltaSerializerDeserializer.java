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
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public final class FlatDeltaSerializerDeserializer extends AbstractDeltaSerializerDeserializer {

	public FlatDeltaSerializerDeserializer(ConcurrentSkipListMap<CausalLogID, ThreadCausalLog> threadCausalLogs,
										   Map<Short, Integer> vertexIDToDistance, int determinantSharingDepth,
										   short localVertexID, BufferPool determinantBufferPool) {
		super(threadCausalLogs, vertexIDToDistance, determinantSharingDepth, localVertexID, determinantBufferPool);
	}

	@Override
	protected void serializeDataStrategy(InputChannelID outputChannelID, long epochID, CompositeByteBuf composite,
										 ByteBuf deltaHeader) {
		CausalLogID outputChannelSpecificCausalLog = outputChannelSpecificCausalLogs.get(outputChannelID);

		for (Map.Entry<CausalLogID, ThreadCausalLog> entry : threadCausalLogs.entrySet()) {
			CausalLogID currCID = entry.getKey();
			ThreadCausalLog log = entry.getValue();
			short currentVertex = currCID.getVertexID();
			int distance = Math.abs(vertexIDToDistance.get(currentVertex));
			if (determinantSharingDepth == -1 || distance + 1 <= determinantSharingDepth) {
				if (currentVertex != localVertexID || currCID.isMainThread() || outputChannelSpecificCausalLog.equals(currCID)) {
					if (log.hasDeltaForConsumer(outputChannelID, epochID)) {
						//serializeID
						serializeCausalLogID(deltaHeader, currCID);
						//serialize delta
						serializeThreadDelta(outputChannelID, epochID, composite, deltaHeader, log, currCID);
					}
				}
			}
		}
	}

	private void serializeCausalLogID(ByteBuf deltaHeader, CausalLogID currCID) {
		deltaHeader.writeShort(currCID.getVertexID());
		deltaHeader.writeBoolean(currCID.isMainThread());
		if (!currCID.isMainThread()) {
			deltaHeader.writeLong(currCID.getIntermediateDataSetLower());
			deltaHeader.writeLong(currCID.getIntermediateDataSetUpper());
			deltaHeader.writeByte(currCID.getSubpartitionIndex());
		}
	}

	@Override
	protected int deserializeStrategyStep(ByteBuf msg, CausalLogID causalLogID, int deltaIndex, long epochID) {
		deserializeCausalLogID(msg, causalLogID);
		deltaIndex += processThreadDelta(msg, causalLogID, deltaIndex, epochID);
		return deltaIndex;
	}

	private void deserializeCausalLogID(ByteBuf msg, CausalLogID causalLogID) {
		short vertexID = msg.readShort();
		boolean hasMainThreadDelta = msg.readBoolean();
		if (hasMainThreadDelta) {
			causalLogID.replace(vertexID);
		} else {
			long intermediateResultPartitionLower = msg.readLong();
			long intermediateResultPartitionUpper = msg.readLong();
			byte subpartitionID = msg.readByte();
			causalLogID.replace(vertexID, intermediateResultPartitionLower, intermediateResultPartitionUpper,
				subpartitionID);
		}
	}

}
