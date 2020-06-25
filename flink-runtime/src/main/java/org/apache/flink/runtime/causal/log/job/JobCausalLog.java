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
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
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

	ConcurrentSet<InputChannelID> registeredConsumers;

	BufferPool bufferPool;

	Object lock;

	DeterminantEncoder encoder;

	public JobCausalLog(VertexGraphInformation vertexGraphInformation, ResultPartitionWriter[] resultPartitionsOfLocalVertex, BufferPool bufferPool, Object lock) {
		this.myVertexID = vertexGraphInformation.getThisTasksVertexID();
		this.bufferPool = bufferPool;

		this.encoder = new SimpleDeterminantEncoder();

		LOG.info("Creating new CausalLoggingManager for id {}, with upstreams {} ", myVertexID, String.join(", ", vertexGraphInformation.getUpstreamVertexes().stream().map(Object::toString).collect(Collectors.toList())));
		localCausalLog = new BasicLocalVertexCausalLog(vertexGraphInformation,resultPartitionsOfLocalVertex, bufferPool, encoder);

		//Defer initializing the determinant logs to avoid having to perform reachability analysis
		this.upstreamDeterminantLogs = new ConcurrentHashMap<>();
		this.registeredConsumers = new ConcurrentSet<>();

		this.lock = lock;
	}



	@Override
	public void registerDownstreamConsumer(InputChannelID inputChannelID, IntermediateResultPartitionID consumedResultPartitionID, int consumedSubpartition) {
		LOG.debug("Registering new input channel at Job level");
		for(VertexCausalLog causalLog : upstreamDeterminantLogs.values())
			causalLog.registerDownstreamConsumer(inputChannelID,consumedResultPartitionID , consumedSubpartition);
		localCausalLog.registerDownstreamConsumer(inputChannelID, consumedResultPartitionID , consumedSubpartition);
		registeredConsumers.add(inputChannelID);
	}

	@Override
	public void appendDeterminant(Determinant determinant, long epochID) {
		assert (Thread.holdsLock(lock));
		LOG.debug("Appending determinant {}", determinant);
		localCausalLog.appendDeterminant(
			determinant,
			epochID
		);
	}

	@Override
	public void appendSubpartitionDeterminant(Determinant determinant, long epochID, IntermediateResultPartitionID intermediateResultPartitionID, int subpartitionIndex) {
		LOG.debug("Appending determinant {} for epochID {} to intermediateDataSetID {} subpartition {}", determinant, epochID, intermediateResultPartitionID, subpartitionIndex);
		localCausalLog.appendSubpartitionDeterminants(
			determinant,
			epochID,
			intermediateResultPartitionID,
			subpartitionIndex
		);
	}

	@Override
	public void processUpstreamVertexCausalLogDelta(VertexCausalLogDelta d, long epochID) {
		upstreamDeterminantLogs
			.computeIfAbsent(d.getVertexId(), k -> new BasicUpstreamVertexCausalLog(k, bufferPool))
			.processUpstreamCausalLogDelta(d, epochID);

	}

	@Override
	public DeterminantEncoder getDeterminantEncoder() {
		return encoder;
	}

	@Override
	public VertexID getVertexId() {
		return myVertexID;
	}

	@Override
	public int mainThreadLogLength() {
		return this.localCausalLog.mainThreadLogLength();
	}

	@Override
	public int subpartitionLogLength(IntermediateResultPartitionID intermediateResultPartitionID, int subpartitionIndex) {
		return this.localCausalLog.subpartitionLogLength(intermediateResultPartitionID, subpartitionIndex);
	}

	@Override
	public void unregisterDownstreamConsumer(InputChannelID toCancel) {
		this.registeredConsumers.remove(toCancel);
		localCausalLog.unregisterDownstreamConsumer(toCancel);
		for(UpstreamVertexCausalLog upstreamVertexCausalLog : upstreamDeterminantLogs.values())
			upstreamVertexCausalLog.unregisterDownstreamConsumer(toCancel);
	}

	@Override
	public VertexCausalLogDelta getDeterminantsOfVertex(VertexID vertexId, long startEpochID) {
		LOG.debug("Got request for determinants of vertexID {}", vertexId);
		return upstreamDeterminantLogs.computeIfAbsent(vertexId, k -> new BasicUpstreamVertexCausalLog(vertexId, bufferPool)).getDeterminants(startEpochID);
	}

	@Override
	public List<VertexCausalLogDelta> getNextDeterminantsForDownstream(InputChannelID inputChannelID, long epochID){
		LOG.debug("Getting deltas to send to downstream channel {} for epochID {}", inputChannelID, epochID);
		List<VertexCausalLogDelta> results = new LinkedList<>();
		for (VertexID key : this.upstreamDeterminantLogs.keySet()) {
			UpstreamVertexCausalLog upstreamVertexCausalLog = upstreamDeterminantLogs.get(key);
			LOG.debug("Upstream log pre processing: {}", upstreamVertexCausalLog);
			VertexCausalLogDelta causalLogDelta = upstreamVertexCausalLog.getNextDeterminantsForDownstream(inputChannelID, epochID);
			if (causalLogDelta.hasUpdates())
				results.add(causalLogDelta);
			LOG.debug("Upstream log post processing: {}", upstreamDeterminantLogs.get(key));
		}
		LOG.debug("Local log pre processing: {}", localCausalLog);
		VertexCausalLogDelta vertexCausalLogDelta = localCausalLog.getNextDeterminantsForDownstream(inputChannelID, epochID);
		LOG.debug("Local log post processing: {}", localCausalLog);
		if(vertexCausalLogDelta.hasUpdates())
			results.add(vertexCausalLogDelta);
		return results;
	}

	@Override
	public synchronized void notifyCheckpointComplete(long checkpointId) throws Exception {
		LOG.debug("Processing checkpoint complete notification for id {}", checkpointId);
		for (UpstreamVertexCausalLog log : upstreamDeterminantLogs.values()) {
			LOG.debug("Determinant log pre processing: {}", log);
			log.notifyCheckpointComplete(checkpointId);
			LOG.debug("Determinant log post processing: {}", log);
		}
		localCausalLog.notifyCheckpointComplete(checkpointId);
	}

}
