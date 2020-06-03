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

import org.apache.flink.runtime.causal.VertexGraphInformation;
import org.apache.flink.runtime.causal.VertexID;
import org.apache.flink.runtime.causal.determinant.*;
import org.apache.flink.runtime.causal.log.vertex.*;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.shaded.netty4.io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/*
Causal log for this operator. Contains Vertex Specific Causal logs for itself and all upstream operators.
 */
public class JobCausalLog implements IJobCausalLog {

	private static final Logger LOG = LoggerFactory.getLogger(JobCausalLog.class);

	VertexID myVertexID;

	ConcurrentMap<VertexID, UpstreamVertexCausalLog> upstreamDeterminantLogs;

	LocalVertexCausalLog localCausalLog;
	DeterminantEncodingStrategy determinantEncodingStrategy;

	ConcurrentSet<InputChannelID> registeredConsumers;

	public JobCausalLog(VertexGraphInformation vertexGraphInformation, ResultPartition[] resultPartitionsOfLocalVertex, BufferPool bufferPool) {

		this.myVertexID = vertexGraphInformation.getThisTasksVertexID();
		LOG.info("Creating new CausalLoggingManager for id {}, with upstreams {} ", myVertexID, String.join(", ", vertexGraphInformation.getUpstreamVertexes().stream().map(Object::toString).collect(Collectors.toList())));
		this.determinantEncodingStrategy = new SimpleDeterminantEncodingStrategy();

		localCausalLog = new BasicLocalVertexCausalLog(vertexGraphInformation,resultPartitionsOfLocalVertex, bufferPool);

		//Defer initializing the determinant logs to avoid having to perform reachability analysis
		this.upstreamDeterminantLogs = new ConcurrentHashMap<>();
		this.registeredConsumers = new ConcurrentSet<>();

	}

	@Override
	public void registerDownstreamConsumer(InputChannelID inputChannelID, IntermediateResultPartitionID consumedResultPartitionID, int consumedSubpartition) {
		LOG.info("Registering new input channel at Job level");
		for(VertexCausalLog causalLog : upstreamDeterminantLogs.values())
			causalLog.registerDownstreamConsumer(inputChannelID,consumedResultPartitionID , consumedSubpartition);
		localCausalLog.registerDownstreamConsumer(inputChannelID, consumedResultPartitionID , consumedSubpartition);
		registeredConsumers.add(inputChannelID);
	}

	@Override
	public void appendDeterminant(Determinant determinant, long checkpointID) {
		LOG.info("Appending determinant {}", determinant);
		localCausalLog.appendDeterminants(
			this.determinantEncodingStrategy.encode(determinant),
			checkpointID
		);
	}

	@Override
	public void appendSubpartitionDeterminants(Determinant determinant, long epochID, IntermediateResultPartitionID intermediateResultPartitionID, int subpartitionIndex) {
		LOG.info("Appending determinant {} for epochID {} to intermediateDataSetID {} subpartition {}", determinant, epochID, intermediateResultPartitionID, subpartitionIndex);
		localCausalLog.appendSubpartitionDeterminants(
			this.determinantEncodingStrategy.encode(determinant),
			epochID,
			intermediateResultPartitionID,
			subpartitionIndex
		);
	}

	@Override
	public void processUpstreamVertexCausalLogDelta(VertexCausalLogDelta d, long checkpointID) {
		upstreamDeterminantLogs
			.computeIfAbsent(d.getVertexId(), BasicUpstreamVertexCausalLog::new)
			.processUpstreamCausalLogDelta(d, checkpointID);

	}

	@Override
	public DeterminantEncodingStrategy getDeterminantEncodingStrategy() {
		return determinantEncodingStrategy;
	}

	@Override
	public VertexID getVertexId() {
		return myVertexID;
	}

	@Override
	public void unregisterDownstreamConsumer(InputChannelID toCancel) {
		this.registeredConsumers.remove(toCancel);
		localCausalLog.unregisterDownstreamConsumer(toCancel);
		for(UpstreamVertexCausalLog upstreamVertexCausalLog : upstreamDeterminantLogs.values())
			upstreamVertexCausalLog.unregisterDownstreamConsumer(toCancel);
	}

	@Override
	public VertexCausalLogDelta getDeterminantsOfVertex(VertexID vertexId) {
		LOG.info("Got request for determinants of vertexID {}", vertexId);
		return upstreamDeterminantLogs.computeIfAbsent(vertexId, BasicUpstreamVertexCausalLog::new).getDeterminants();
	}

	@Override
	public List<VertexCausalLogDelta> getNextDeterminantsForDownstream(InputChannelID inputChannelID, long epochID){
		LOG.info("Getting deltas to send to downstream channel {} for epochID {}", inputChannelID, epochID);
		List<VertexCausalLogDelta> results = new LinkedList<>();
		for (VertexID key : this.upstreamDeterminantLogs.keySet()) {
			UpstreamVertexCausalLog upstreamVertexCausalLog = upstreamDeterminantLogs.get(key);
			LOG.info("Determinant log pre processing: {}", upstreamVertexCausalLog);
			VertexCausalLogDelta causalLogDelta = upstreamVertexCausalLog.getNextDeterminantsForDownstream(inputChannelID, epochID);
			if (causalLogDelta.hasUpdates())
				results.add(causalLogDelta);
			LOG.info("Determinant log post processing: {}", upstreamDeterminantLogs.get(key));
		}
		VertexCausalLogDelta vertexCausalLogDelta = localCausalLog.getNextDeterminantsForDownstream(inputChannelID, epochID);
		if(vertexCausalLogDelta.hasUpdates())
			results.add(vertexCausalLogDelta);
		return results;
	}

	@Override
	public synchronized void notifyCheckpointComplete(long checkpointId) throws Exception {
		LOG.info("Processing checkpoint complete notification for id {}", checkpointId);
		for (UpstreamVertexCausalLog log : upstreamDeterminantLogs.values()) {
			LOG.info("Determinant log pre processing: {}", log);
			log.notifyCheckpointComplete(checkpointId);
			LOG.info("Determinant log post processing: {}", log);
		}
		localCausalLog.notifyCheckpointComplete(checkpointId);
	}

}
