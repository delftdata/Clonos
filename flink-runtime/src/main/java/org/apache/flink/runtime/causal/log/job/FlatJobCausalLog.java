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
import org.apache.flink.runtime.causal.determinant.Determinant;
import org.apache.flink.runtime.causal.determinant.DeterminantEncoder;
import org.apache.flink.runtime.causal.determinant.SimpleDeterminantEncoder;
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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class FlatJobCausalLog implements JobCausalLog {
	private static final Logger LOG = LoggerFactory.getLogger(FlatJobCausalLog.class);


	private final DeterminantEncoder encoder;

	private final int determinantSharingDepth;

	//TODO now we are boxing shorts to send downstream... We can have another list of the logs to share downstream
	private final Map<Short, Integer> vertexIDToDistance;

	private final ConcurrentSkipListMap<CausalLogID, ThreadCausalLog> threadCausalLogs;

	private final CausalLogID localMainThreadCausalLogID;

	private final short localVertexID;

	private final DeltaSerializerDeserializer deltaSerdeStrategy;

	public FlatJobCausalLog(VertexGraphInformation vertexGraphInformation, int determinantSharingDepth,
							ResultPartitionWriter[] resultPartitionsOfLocalVertex, BufferPool bufferPool,
							DeltaEncodingStrategy deltaEncodingStrategy) {
		this.vertexIDToDistance =
			vertexGraphInformation.getDistances().entrySet().stream().collect(Collectors.toMap(e -> e.getKey().getVertexId(), Map.Entry::getValue));
		this.localVertexID = vertexGraphInformation.getThisTasksVertexID().getVertexId();
		this.determinantSharingDepth = determinantSharingDepth;
		this.encoder = new SimpleDeterminantEncoder();

		short localVertexID = vertexGraphInformation.getThisTasksVertexID().getVertexId();
		this.localMainThreadCausalLogID = new CausalLogID(localVertexID);

		//Create thread logs for local task main thread and subpartition threads.
		threadCausalLogs = new ConcurrentSkipListMap<>();
		registerLocalThreadLogs(resultPartitionsOfLocalVertex, bufferPool, localVertexID);

		if (deltaEncodingStrategy.equals(DeltaEncodingStrategy.FLAT))
			this.deltaSerdeStrategy = new FlatDeltaSerializerDeserializer(threadCausalLogs, vertexIDToDistance,
				determinantSharingDepth, localVertexID, bufferPool);
		else
			this.deltaSerdeStrategy = new GroupingDeltaSerializerDeserializer(threadCausalLogs, vertexIDToDistance,
				determinantSharingDepth, localVertexID, bufferPool);
	}

	private void registerLocalThreadLogs(ResultPartitionWriter[] resultPartitionsOfLocalVertex, BufferPool bufferPool,
										 short localVertexID) {
		//Register the main thread log
		threadCausalLogs.put(localMainThreadCausalLogID, new ThreadCausalLogImpl(bufferPool, encoder));

		//Register the Partitions
		for (ResultPartitionWriter writer : resultPartitionsOfLocalVertex) {
			IntermediateResultPartitionID intermediateResultPartitionID = writer.getPartitionId().getPartitionId();
			long partitionIDLower = intermediateResultPartitionID.getLowerPart();
			long partitionIDUpper = intermediateResultPartitionID.getUpperPart();

			//Register the subpartitions
			for (int i = 0; i < writer.getNumberOfSubpartitions(); i++) {
				CausalLogID subpartitionCausalLogID = new CausalLogID(localVertexID, partitionIDLower,
					partitionIDUpper, (byte) i);
				threadCausalLogs.put(subpartitionCausalLogID, new ThreadCausalLogImpl(bufferPool, encoder));
			}
		}
	}


	@Override
	public short getLocalVertexID() {
		return localVertexID;
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
	public void appendDeterminant(CausalLogID causalLogID, Determinant determinant, long epochID) {
		LOG.info("Appending Determinant {} to CausalLog {}", determinant, causalLogID);
		threadCausalLogs.get(causalLogID).appendDeterminant(determinant, epochID);
	}

	@Override
	public void appendDeterminant(Determinant determinant, long epochID) {
		//assert (Thread.holdsLock(lock));
		LOG.info("Appending Determinant {} to main thread CausalLog {}", determinant, localMainThreadCausalLogID);
		threadCausalLogs.get(localMainThreadCausalLogID).appendDeterminant(determinant, epochID);

	}

	@Override
	public DeterminantResponseEvent respondToDeterminantRequest(VertexID vertexId, long startEpochID) {
		LOG.debug("Got request for determinants of vertexID {}", vertexId);
		if (determinantSharingDepth != -1 && Math.abs(this.vertexIDToDistance.get(vertexId.getVertexId())) > determinantSharingDepth)
			return new DeterminantResponseEvent(false, vertexId);
		else {
			short vertex = vertexId.getVertexId();
			Map<CausalLogID, ByteBuf> determinants = new HashMap<>();
			for (Map.Entry<CausalLogID, ThreadCausalLog> e : threadCausalLogs.entrySet()) {
				if (e.getKey().isForVertex(vertex)) {
					ByteBuf buf = e.getValue().getDeterminants(startEpochID);
					determinants.put(e.getKey(), buf);
				}
			}
			return new DeterminantResponseEvent(vertexId, determinants);
		}
	}

	@Override
	public void registerDownstreamConsumer(InputChannelID outputChannelID,
										   IntermediateResultPartitionID intermediateResultPartitionID,
										   int consumedSubpartition) {
		CausalLogID consumedCausalLog = new CausalLogID(this.localVertexID,
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
		for (ThreadCausalLog threadCausalLog : threadCausalLogs.values()) {
			threadCausalLog.notifyCheckpointComplete(checkpointID);
		}
	}

	@Override
	public void close() {
		for (ThreadCausalLog threadCausalLog : threadCausalLogs.values()) {
			threadCausalLog.close();
		}
	}

	@Override
	public int mainThreadLogLength() {
		CausalLogID mainThreadID = new CausalLogID(localVertexID);
		return threadCausalLogs.get(mainThreadID).logLength();
	}

	@Override
	public int subpartitionLogLength(IntermediateResultPartitionID intermediateResultPartitionID,
									 int subpartitionIndex) {
		CausalLogID subpartitionID = new CausalLogID(localVertexID, intermediateResultPartitionID.getLowerPart(),
			intermediateResultPartitionID.getUpperPart(), (byte) subpartitionIndex);
		return threadCausalLogs.get(subpartitionID).logLength();
	}
}
