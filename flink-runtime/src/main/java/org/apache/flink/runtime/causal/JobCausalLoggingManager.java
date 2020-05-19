/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.causal;

import org.apache.flink.runtime.causal.determinant.*;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/*
Causal log for this operator. Contains Vertex Specific Causal logs for itself and all upstream operators.
 */
public class JobCausalLoggingManager implements IJobCausalLoggingManager {

	private static final Logger LOG = LoggerFactory.getLogger(JobCausalLoggingManager.class);

	VertexId myVertexId;
	Map<VertexId, CausalLog> determinantLogs;
	DeterminantEncodingStrategy determinantEncodingStrategy;

	public JobCausalLoggingManager(VertexGraphInformation vertexGraphInformation) {
		this(vertexGraphInformation.getThisTasksVertexId(), vertexGraphInformation.getUpstreamVertexes());
	}

	public JobCausalLoggingManager(VertexId myVertexId, Collection<VertexId> upstreamVertexIds) {
		LOG.info("Creating new CausalLoggingManager for id {}, with upstreams {} ", myVertexId, String.join(", ", upstreamVertexIds.stream().map(Object::toString).collect(Collectors.toList())));
		this.determinantLogs = new HashMap<>();
		this.myVertexId = myVertexId;

		for (VertexId u : upstreamVertexIds)
			determinantLogs.put(u, new CircularCausalLog(u));
		this.determinantLogs.put(this.myVertexId, new CircularCausalLog(this.myVertexId));

		this.determinantEncodingStrategy = new SimpleDeterminantEncodingStrategy();
	}


	@Override
	public void registerDownstreamConsumer(InputChannelID inputChannelID) {
		for(CausalLog circularVertexCausalLog : determinantLogs.values())
			circularVertexCausalLog.registerDownstreamConsumer(inputChannelID);
	}

	@Override
	public List<CausalLogDelta> getDeterminants() {
		List<CausalLogDelta> results = new LinkedList<>();
		for (VertexId key : this.determinantLogs.keySet()) {
			results.add(new CausalLogDelta(key, determinantLogs.get(key).getDeterminants(), 0));
		}
		results.add(new CausalLogDelta(this.myVertexId, determinantLogs.get(this.myVertexId).getDeterminants(), 0));
		return results;
	}


	@Override
	public void appendDeterminant(Determinant determinant) {
		LOG.info("Appending determinant {}", determinant);
		this.determinantLogs.get(this.myVertexId).appendDeterminants(
			this.determinantEncodingStrategy.encode(determinant)
		);
	}

	@Override
	public void processCausalLogDelta(CausalLogDelta d) {
		LOG.info("Processing UpstreamCausalLogDelta {}", d);
		LOG.info("Determinant log pre processing: {}", this.determinantLogs.get(d.vertexId));
		this.determinantLogs.get(d.vertexId).processUpstreamVertexCausalLogDelta(d);
		LOG.info("Determinant log post processing: {}", this.determinantLogs.get(d.vertexId));
	}

	@Override
	public void notifyCheckpointBarrier(long checkpointId) {
		LOG.info("Processing checkpoint barrier {}", checkpointId);
		for (CausalLog log : determinantLogs.values()) {
			LOG.info("Determinant log pre processing: {}", log);
			log.notifyCheckpointBarrier(checkpointId);
			LOG.info("Determinant log post processing: {}", log);
		}
	}

	@Override
	public DeterminantEncodingStrategy getDeterminantEncodingStrategy() {
		return determinantEncodingStrategy;
	}

	@Override
	public VertexId getVertexId() {
		return myVertexId;
	}

	@Override
	public void unregisterDownstreamConsumer(InputChannelID toCancel) {
		for(CausalLog vcl : determinantLogs.values())
			vcl.unregisterDownstreamConsumer(toCancel);
	}

	@Override
	public byte[] getDeterminantsOfVertex(VertexId vertexId) {
		return determinantLogs.get(vertexId).getDeterminants();
	}

	@Override
	public List<CausalLogDelta> getNextDeterminantsForDownstream(InputChannelID inputChannelID){
		LOG.info("Getting deltas to send to downstream channel {}", inputChannelID);
		List<CausalLogDelta> results = new LinkedList<>();
		for (VertexId key : this.determinantLogs.keySet()) {
			LOG.info("Determinant log pre processing: {}", determinantLogs.get(key));
			CausalLogDelta causalLogDelta = determinantLogs.get(key).getNextDeterminantsForDownstream(inputChannelID);
			if (causalLogDelta.rawDeterminants.length != 0)
				results.add(causalLogDelta);
			LOG.info("Determinant log post processing: {}", determinantLogs.get(key));
		}
		return results;
	}

	@Override
	public synchronized void notifyCheckpointComplete(long checkpointId) throws Exception {
		LOG.info("Processing checkpoint complete notification for id {}", checkpointId);
		for (CausalLog log : determinantLogs.values()) {
			LOG.info("Determinant log pre processing: {}", log);
			log.notifyCheckpointComplete(checkpointId);
			LOG.info("Determinant log post processing: {}", log);
		}
	}

}
