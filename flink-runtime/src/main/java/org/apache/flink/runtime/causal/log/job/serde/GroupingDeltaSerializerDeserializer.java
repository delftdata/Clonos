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
import org.apache.flink.runtime.causal.log.job.hierarchy.PartitionCausalLogs;
import org.apache.flink.runtime.causal.log.job.hierarchy.VertexCausalLogs;
import org.apache.flink.runtime.causal.log.thread.ThreadCausalLog;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.util.internal.ConcurrentSet;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;


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


	public GroupingDeltaSerializerDeserializer(ConcurrentMap<CausalLogID, ThreadCausalLog> threadCausalLogs,
											   ConcurrentMap<Short, VertexCausalLogs> hierarchicalThreadCausalLogsToBeShared,
											   Map<Short, Integer> vertexIDToDistance,
											   ConcurrentSet<Short> localVertices, int determinantSharingDepth,
											   BufferPool determinantBufferPool) {
		super(threadCausalLogs, hierarchicalThreadCausalLogsToBeShared, vertexIDToDistance, localVertices, determinantSharingDepth,
			determinantBufferPool);
	}


	@Override
	protected int deserializeStrategyStep(ByteBuf msg, CausalLogID causalLogID, int deltaIndex, long epochID) {
		short vertexID = msg.readShort();
		causalLogID.replace(vertexID);
		boolean hasMainThreadDelta = msg.readBoolean();
		int deltaIndexOffset = 0;
		if (hasMainThreadDelta)
			deltaIndexOffset += processThreadDelta(msg, causalLogID, deltaIndex, epochID);

		byte numPartitionDeltas = msg.readByte();
		for (int p = 0; p < numPartitionDeltas; p++) {
			long intermediateResultPartitionLower = msg.readLong();
			long intermediateResultPartitionUpper = msg.readLong();
			byte numSubpartitionDeltas = msg.readByte();
			for (int s = 0; s < numSubpartitionDeltas; s++) {
				byte subpartitionID = msg.readByte();
				causalLogID.replace(intermediateResultPartitionLower, intermediateResultPartitionUpper,
					subpartitionID);
				deltaIndexOffset += processThreadDelta(msg, causalLogID, deltaIndex + deltaIndexOffset, epochID);
			}
		}
		return deltaIndexOffset;
	}


	@Override
	protected void serializeDataStrategy(InputChannelID outputChannelID, long epochID, CompositeByteBuf composite,
										 ByteBuf deltaHeader) {
		CausalLogID outputChannelSpecificCausalLog = outputChannelSpecificCausalLogs.get(outputChannelID);
		for (Map.Entry<Short, VertexCausalLogs> e : hierarchicalThreadCausalLogsToBeShared.entrySet())
			//If this is not a local vertex or if it is the local vertex that this channel consumes directly
			if(!localVertices.contains(e.getKey()) || outputChannelSpecificCausalLog.isForVertex(e.getKey()))
				serializeVertex(outputChannelID, epochID, composite, deltaHeader, outputChannelSpecificCausalLog, e.getValue());


	}

	private void serializeVertex(InputChannelID outputChannelID, long epochID, CompositeByteBuf composite,
								 ByteBuf deltaHeader, CausalLogID outputChannelSpecificCausalLog, VertexCausalLogs v) {
		int numVertexUpdates = 0;
		int vertexStartIndex = deltaHeader.writerIndex(); //May need to erase entire vertex
		short vertexID = v.getVertexID();
		deltaHeader.writeShort(vertexID);
		ThreadCausalLog mainThreadLog = v.mainThreadLog.get();
		if (mainThreadLog != null && mainThreadLog.hasDeltaForConsumer(outputChannelID, epochID)) {
			deltaHeader.writeBoolean(true);
			serializeThreadDelta(outputChannelID, epochID, composite, deltaHeader, mainThreadLog);
			numVertexUpdates++;
		} else
			deltaHeader.writeBoolean(false);

		numVertexUpdates += serializePartitions(outputChannelID, epochID, composite, deltaHeader,
			outputChannelSpecificCausalLog, v, vertexID);

		if (numVertexUpdates == 0) //If there were no updates, reset the writer index.
			deltaHeader.writerIndex(vertexStartIndex);
	}

	private int serializePartitions(InputChannelID outputChannelID, long epochID, CompositeByteBuf composite,
									ByteBuf deltaHeader, CausalLogID outputChannelSpecificCausalLog,
									VertexCausalLogs v, short vertexID) {
		int numUpdatesOnAllPartitions = 0;
		int partitionSectionStartIndex = deltaHeader.writerIndex();
		deltaHeader.writeByte(0);//Num partition deltas
		for (PartitionCausalLogs p : v.partitionCausalLogs.values())
			numUpdatesOnAllPartitions += serializePartition(outputChannelID, epochID, composite, deltaHeader,
				outputChannelSpecificCausalLog, vertexID, p);

		//Even if numUpdatesOnAllPartitions is zero, we need to write it there.
		deltaHeader.setByte(partitionSectionStartIndex, numUpdatesOnAllPartitions);

		return numUpdatesOnAllPartitions;
	}

	private int serializePartition(InputChannelID outputChannelID, long epochID, CompositeByteBuf composite,
								   ByteBuf deltaHeader, CausalLogID outputChannelSpecificCausalLog, short vertexID,
								   PartitionCausalLogs p) {
		int specificPartitionStartIndex = deltaHeader.writerIndex();
		deltaHeader.writeLong(p.intermediateResultPartitionID.getLowerPart());
		deltaHeader.writeLong(p.intermediateResultPartitionID.getUpperPart());

		int numSubpartitionUpdates = 0;
		int numSubpartitionUpdatesIndex = deltaHeader.writerIndex();
		deltaHeader.writeByte(0); //num subpartition updates
		for (ThreadCausalLog s : p.subpartitionLogs.values()) {
			if (s.hasDeltaForConsumer(outputChannelID, epochID))
				//If this isnt the local vertex or if it is the specific channel subpartition
				if (!outputChannelSpecificCausalLog.isForVertex(vertexID) || outputChannelSpecificCausalLog.equals(s.getCausalLogID())) {
					deltaHeader.writeByte(s.getCausalLogID().getSubpartitionIndex());
					serializeThreadDelta(outputChannelID, epochID, composite, deltaHeader, s);
					numSubpartitionUpdates++;
				}
		}
		if (numSubpartitionUpdates == 0) {
			deltaHeader.writerIndex(specificPartitionStartIndex);
			return 0;
		} else {
			deltaHeader.setByte(numSubpartitionUpdatesIndex, numSubpartitionUpdates);
			return 1;
		}
	}


}
