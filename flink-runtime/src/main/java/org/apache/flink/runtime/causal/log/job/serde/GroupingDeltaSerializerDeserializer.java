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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;


/**
 * This class holds the complex serialization logic for causal deltas that get piggybacked on buffers.
 * <p>
 * The reason for the complexity of the logic is due to the flattened structure of the job causal log.
 * The previous version was much simpler and easier to understand, but it required a large amount of intermediate
 * materialization and object creation.
 * <p>
 * Deserialization is relatively simple and unchanged.
 */
public final class GroupingDeltaSerializerDeserializer extends AbstractDeltaSerializerDeserializer implements DeltaSerializerDeserializer {


	public GroupingDeltaSerializerDeserializer(ConcurrentSkipListMap<CausalLogID, ThreadCausalLog> threadCausalLogs,
											   Map<Short, Integer> vertexIDToDistance, int determinantSharingDepth,
											   short localVertexID, BufferPool determinantBufferPool) {
		super(threadCausalLogs, vertexIDToDistance, determinantSharingDepth, localVertexID, determinantBufferPool);
	}


	@Override
	protected int deserializeStrategyStep(ByteBuf msg, CausalLogID causalLogID, int deltaIndex, long epochID) {
		short vertexID = msg.readShort();
		causalLogID.replace(vertexID);
		boolean hasMainThreadDelta = msg.readBoolean();

		if (hasMainThreadDelta)
			deltaIndex += processThreadDelta(msg, causalLogID, deltaIndex, epochID);

		byte numPartitionDeltas = msg.readByte();
		for (int p = 0; p < numPartitionDeltas; p++) {
			long intermediateResultPartitionLower = msg.readLong();
			long intermediateResultPartitionUpper = msg.readLong();
			byte numSubpartitionDeltas = msg.readByte();
			for (int s = 0; s < numSubpartitionDeltas; s++) {
				byte subpartitionID = msg.readByte();
				causalLogID.replace(intermediateResultPartitionLower, intermediateResultPartitionUpper,
					subpartitionID);
				deltaIndex += processThreadDelta(msg, causalLogID, deltaIndex, epochID);
			}
		}
		return deltaIndex;
	}


	@Override
	protected void serializeDataStrategy(InputChannelID outputChannelID, long epochID, CompositeByteBuf composite,
										 ByteBuf deltaHeader) {

		Iterator<Map.Entry<CausalLogID, ThreadCausalLog>> iterator = threadCausalLogs.entrySet().iterator();
		Map.Entry<CausalLogID, ThreadCausalLog> vertexEntry = iterator.next();

		while (iterator.hasNext()) {
			short currentVertex = vertexEntry.getKey().getVertexID();
			int distance = Math.abs(vertexIDToDistance.get(currentVertex));

			//If this vertex is supposed to be shared downstream
			if (determinantSharingDepth == -1 || distance + 1 <= determinantSharingDepth)
				vertexEntry = serializeVertex(outputChannelID, epochID, composite, deltaHeader, iterator, vertexEntry);
			else
				while (vertexEntry.getKey().getVertexID() == currentVertex && iterator.hasNext()) //Skip till next
					// vertex
					vertexEntry = iterator.next();
		}
	}

	private Map.Entry<CausalLogID, ThreadCausalLog> serializeVertex(InputChannelID outputChannelID, long epochID,
																	CompositeByteBuf composite,
																	ByteBuf deltaHeader,
																	Iterator<Map.Entry<CausalLogID, ThreadCausalLog>> iterator,
																	Map.Entry<CausalLogID, ThreadCausalLog> vertexEntry) {
		CausalLogID vertexCID = vertexEntry.getKey();
		ThreadCausalLog vertexLog = vertexEntry.getValue();

		deltaHeader.writeShort(vertexCID.getVertexID());

		boolean hasMainThreadDelta = vertexLog.hasDeltaForConsumer(outputChannelID, epochID);
		deltaHeader.writeBoolean(hasMainThreadDelta);
		if (hasMainThreadDelta)
			serializeThreadDelta(outputChannelID, epochID, composite, deltaHeader, vertexLog,vertexCID);

		vertexEntry = serializePartitions(deltaHeader, composite, iterator, vertexEntry, outputChannelID, epochID);

		return vertexEntry;
	}

	private Map.Entry<CausalLogID, ThreadCausalLog> serializePartitions(ByteBuf deltaHeader,
																		CompositeByteBuf composite,
																		Iterator<Map.Entry<CausalLogID,
																			ThreadCausalLog>> iterator,
																		Map.Entry<CausalLogID, ThreadCausalLog> vertexEntry, InputChannelID outputChannelID, long epochID) {
		CausalLogID vertexCID = vertexEntry.getKey();

		int numPartitionUpdates = 0;
		int numPartitionUpdatesWriteIndex = deltaHeader.writerIndex();
		deltaHeader.writeByte(0); //NumPartitionUpdates

		CausalLogID outputChannelSpecificCausalLog = outputChannelSpecificCausalLogs.get(outputChannelID);
		//Invariant: Each vertex has at least one output partition
		while (iterator.hasNext()) {
			Map.Entry<CausalLogID, ThreadCausalLog> partitionEntry = iterator.next();
			CausalLogID partitionCID = partitionEntry.getKey();

			deltaHeader.writeLong(partitionCID.getIntermediateDataSetLower());
			deltaHeader.writeLong(partitionCID.getIntermediateDataSetUpper());

			Map.Entry<CausalLogID, ThreadCausalLog> subpartitionEntry = partitionEntry;

			int numSubpartitionUpdates = 0;
			int numSubpartitionUpdatesWriteIndex = deltaHeader.writerIndex();
			deltaHeader.writeByte(0); //NumSubpartitionUpdates

			CausalLogID subpartitionCID = partitionEntry.getKey();
			ThreadCausalLog subpartitionLog = subpartitionEntry.getValue();
			if (!subpartitionCID.isForVertex(localVertexID) || outputChannelSpecificCausalLog.equals(subpartitionCID)) { //If this isnt the local vertex or if it is the specific channel subpartition
				if (subpartitionLog.hasDeltaForConsumer(outputChannelID, epochID)) {
					serializeSubpartition(deltaHeader, composite, outputChannelID, epochID, subpartitionCID,
						subpartitionLog);
					numSubpartitionUpdates++;
				}
			}

			while (iterator.hasNext()) {
				subpartitionEntry = iterator.next();
				subpartitionCID = partitionEntry.getKey();
				subpartitionLog = subpartitionEntry.getValue();
				if (!partitionCID.isForVertex(vertexCID.getVertexID()) ||
					!subpartitionCID.isForIntermediatePartition(partitionCID.getIntermediateDataSetLower(),
						partitionCID.getIntermediateDataSetUpper())) {
					partitionEntry = subpartitionEntry;
					break;
				}
				if (!subpartitionCID.isForVertex(localVertexID) || outputChannelSpecificCausalLogs.get(outputChannelID).equals(subpartitionCID)) { //If this isnt the local vertex or if it is the specific channel subpartition
					if (subpartitionLog.hasDeltaForConsumer(outputChannelID, epochID)) {
						serializeSubpartition(deltaHeader, composite, outputChannelID, epochID, subpartitionCID,
							subpartitionLog);
						numSubpartitionUpdates++;
					}
				}
			}

			if (numSubpartitionUpdates == 0) {
				deltaHeader.writerIndex(numSubpartitionUpdatesWriteIndex - 8 * 2); // Erase partition
			} else {
				numPartitionUpdates++;
				deltaHeader.setByte(numSubpartitionUpdatesWriteIndex, numSubpartitionUpdates);
			}

			partitionCID = partitionEntry.getKey();
			if (!partitionCID.isForVertex(vertexCID.getVertexID())) {
				vertexEntry = partitionEntry;
				break;
			}

		}
		deltaHeader.setByte(numPartitionUpdatesWriteIndex, numPartitionUpdates);
		return vertexEntry;
	}

	private void serializeSubpartition(ByteBuf deltaHeader, CompositeByteBuf composite, InputChannelID outputChannelID
		, long epochID, CausalLogID subpartitionCID, ThreadCausalLog subpartitionLog) {
		deltaHeader.writeByte(subpartitionCID.getSubpartitionIndex());
		serializeThreadDelta(outputChannelID, epochID, composite, deltaHeader, subpartitionLog, subpartitionCID);


	}
}
