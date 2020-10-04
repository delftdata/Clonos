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

package org.apache.flink.runtime.causal.log.job;

import org.apache.flink.runtime.causal.DeterminantResponseEvent;
import org.apache.flink.runtime.causal.VertexGraphInformation;
import org.apache.flink.runtime.causal.VertexID;
import org.apache.flink.runtime.causal.determinant.DeterminantEncoder;
import org.apache.flink.runtime.causal.determinant.SimpleDeterminantEncoder;
import org.apache.flink.runtime.causal.log.job.hierarchy.PartitionCausalLogs;
import org.apache.flink.runtime.causal.log.job.hierarchy.VertexCausalLogs;
import org.apache.flink.runtime.causal.log.job.serde.DeltaEncodingStrategy;
import org.apache.flink.runtime.causal.log.job.serde.DeltaSerializerDeserializer;
import org.apache.flink.runtime.causal.log.job.serde.FlatDeltaSerializerDeserializer;
import org.apache.flink.runtime.causal.log.job.serde.GroupingDeltaSerializerDeserializer;
import org.apache.flink.runtime.causal.log.thread.ThreadCausalLog;
import org.apache.flink.runtime.causal.log.thread.ThreadCausalLogImpl;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * This implementation of the {@link JobCausalLog} maintains both a flat and a hierarchical data-structure of the
 * {@link ThreadCausalLog}s of the job.
 * The Flat data-structure is used for accesses by {@link CausalLogID}, while the hierarchical data-structure is
 * maintained for easy serialization and deserialization using the {@link GroupingDeltaSerializerDeserializer}, which saves on metadata
 * transmitted.
 *
 * To avoid auto-boxing concerns with the vertexIDToDistance map, we also only store the logs to share downstream
 * in the hierarchical structure.
 */
public class JobCausalLogImpl implements JobCausalLog {
	private static final Logger LOG = LoggerFactory.getLogger(JobCausalLogImpl.class);


	private final DeterminantEncoder encoder;

	private final int determinantSharingDepth;

	private final Map<Short, Integer> vertexIDToDistance;

	private final ConcurrentMap<CausalLogID, ThreadCausalLog> flatThreadCausalLogs;

	// Stores the logs to be shared downstream (regarding determinant sharing depth), in a hierarchical fashion
	private final ConcurrentMap<Short, VertexCausalLogs> hierarchicalThreadCausalLogsToBeShared;

	private final CausalLogID localMainThreadCausalLogID;

	private final DeltaSerializerDeserializer deltaSerdeStrategy;

	public JobCausalLogImpl(VertexGraphInformation vertexGraphInformation, int determinantSharingDepth,
							ResultPartitionWriter[] resultPartitionsOfLocalVertex, BufferPool bufferPool,
							DeltaEncodingStrategy deltaEncodingStrategy) {
		this.vertexIDToDistance =
			vertexGraphInformation.getDistances().entrySet().stream().collect(Collectors.toMap(e -> e.getKey().getVertexId(), Map.Entry::getValue));
		this.determinantSharingDepth = determinantSharingDepth;
		this.encoder = new SimpleDeterminantEncoder();

		short localVertexID = vertexGraphInformation.getThisTasksVertexID().getVertexId();
		this.localMainThreadCausalLogID = new CausalLogID(localVertexID);

		//Create thread logs for local task main thread and subpartition threads.
		flatThreadCausalLogs = new ConcurrentHashMap<>();
		hierarchicalThreadCausalLogsToBeShared = new ConcurrentHashMap<>();
		registerLocalThreadLogs(resultPartitionsOfLocalVertex, bufferPool, localVertexID);

		if (deltaEncodingStrategy.equals(DeltaEncodingStrategy.FLAT))
			this.deltaSerdeStrategy = new FlatDeltaSerializerDeserializer(flatThreadCausalLogs,
				hierarchicalThreadCausalLogsToBeShared, vertexIDToDistance, determinantSharingDepth, localVertexID,
				bufferPool);
		else
			this.deltaSerdeStrategy = new GroupingDeltaSerializerDeserializer(flatThreadCausalLogs,
				hierarchicalThreadCausalLogsToBeShared, vertexIDToDistance, determinantSharingDepth, localVertexID,
				bufferPool);
	}

	private void registerLocalThreadLogs(ResultPartitionWriter[] resultPartitionsOfLocalVertex, BufferPool bufferPool,
										 short localVertexID) {
		//Register the main thread log
		ThreadCausalLog mainThreadLog = new ThreadCausalLogImpl(bufferPool, localMainThreadCausalLogID, encoder);
		flatThreadCausalLogs.put(localMainThreadCausalLogID, mainThreadLog);
		VertexCausalLogs hierarchicalVertexCausalLogs = null;
		if (determinantSharingDepth != 0) {
			hierarchicalVertexCausalLogs = new VertexCausalLogs(localVertexID);
			hierarchicalThreadCausalLogsToBeShared.put(localVertexID, hierarchicalVertexCausalLogs);
			hierarchicalVertexCausalLogs.mainThreadLog.set(mainThreadLog);
		}

		//Register the Partitions
		for (ResultPartitionWriter writer : resultPartitionsOfLocalVertex) {
			IntermediateResultPartitionID intermediateResultPartitionID = writer.getPartitionId().getPartitionId();
			long partitionIDLower = intermediateResultPartitionID.getLowerPart();
			long partitionIDUpper = intermediateResultPartitionID.getUpperPart();
			PartitionCausalLogs hierarchicalPartitionCausalLogs = null;
			if (determinantSharingDepth != 0) {
				hierarchicalPartitionCausalLogs = new PartitionCausalLogs(intermediateResultPartitionID);
				hierarchicalVertexCausalLogs.partitionCausalLogs.put(intermediateResultPartitionID,
					hierarchicalPartitionCausalLogs);
			}

			//Register the subpartitions
			for (int i = 0; i < writer.getNumberOfSubpartitions(); i++) {
				CausalLogID subpartitionCausalLogID = new CausalLogID(localVertexID, partitionIDLower,
					partitionIDUpper, (byte) i);
				ThreadCausalLog subpartitionThreadCausalLog = new ThreadCausalLogImpl(bufferPool,
					subpartitionCausalLogID, encoder);
				flatThreadCausalLogs.put(subpartitionCausalLogID, subpartitionThreadCausalLog);
				if(determinantSharingDepth != 0)
					hierarchicalPartitionCausalLogs.subpartitionLogs.put((byte) i, subpartitionThreadCausalLog);
			}
		}
	}

	@Override
	public short getLocalVertexID() {
		return localMainThreadCausalLogID.getVertexID();
	}

	@Override
	public ThreadCausalLog getThreadCausalLog(CausalLogID causalLogID) {
		return flatThreadCausalLogs.get(causalLogID);
	}

	@Override
	public void processCausalLogDelta(ByteBuf msg) {
		deltaSerdeStrategy.processCausalLogDelta(msg);
	}

	@Override
	public ByteBuf enrichWithCausalLogDelta(ByteBuf serialized, InputChannelID outputChannelID, long epochID) {
		return deltaSerdeStrategy.enrichWithCausalLogDelta(serialized, outputChannelID, epochID);
	}

	@Override
	public DeterminantResponseEvent respondToDeterminantRequest(VertexID vertexId, long startEpochID) {
		LOG.debug("Got request for determinants of vertexID {}", vertexId);
		if (determinantSharingDepth != -1 && Math.abs(this.vertexIDToDistance.get(vertexId.getVertexId())) > determinantSharingDepth)
			return new DeterminantResponseEvent(false, vertexId);
		else {
			short vertex = vertexId.getVertexId();
			Map<CausalLogID, ByteBuf> determinants = new HashMap<>();

			for (Map.Entry<CausalLogID, ThreadCausalLog> e : flatThreadCausalLogs.entrySet())
				if (e.getKey().isForVertex(vertex))
					determinants.put(e.getKey(), e.getValue().getDeterminants(startEpochID));

			return new DeterminantResponseEvent(vertexId, determinants);
		}
	}

	@Override
	public void registerDownstreamConsumer(InputChannelID outputChannelID,
										   IntermediateResultPartitionID intermediateResultPartitionID,
										   int consumedSubpartition) {
		CausalLogID consumedCausalLog = new CausalLogID(this.localMainThreadCausalLogID.getVertexID(),
			intermediateResultPartitionID.getLowerPart(),
			intermediateResultPartitionID.getUpperPart(), (byte) consumedSubpartition);

		deltaSerdeStrategy.registerDownstreamConsumer(outputChannelID, consumedCausalLog);
	}

	@Override
	public void unregisterDownstreamConsumer(InputChannelID toCancel) {
		//TODO- is anything necessary really?
	}

	@Override
	public DeterminantEncoder getDeterminantEncoder() {
		return encoder;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointID) {
		for (ThreadCausalLog threadCausalLog : flatThreadCausalLogs.values()) {
			threadCausalLog.notifyCheckpointComplete(checkpointID);
		}
	}

	@Override
	public void close() {
		for (ThreadCausalLog threadCausalLog : flatThreadCausalLogs.values()) {
			threadCausalLog.close();
		}
	}

	@Override
	public int mainThreadLogLength() {
		return flatThreadCausalLogs.get(localMainThreadCausalLogID).logLength();
	}

	@Override
	public int subpartitionLogLength(IntermediateResultPartitionID intermediateResultPartitionID,
									 int subpartitionIndex) {
		CausalLogID subpartitionID = new CausalLogID(localMainThreadCausalLogID.getVertexID(),
			intermediateResultPartitionID.getLowerPart(),
			intermediateResultPartitionID.getUpperPart(), (byte) subpartitionIndex);
		return flatThreadCausalLogs.get(subpartitionID).logLength();
	}


}
