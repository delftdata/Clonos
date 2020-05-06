/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional debugrmation
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
import org.apache.flink.runtime.causal.recovery.State;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/*
Causal log for this operator. Contains Vertex Specific Causal logs for itself and all upstream operators.
 */
public class CausalLoggingManager implements ICausalLoggingManager {

	private static final Logger LOG = LoggerFactory.getLogger(CausalLoggingManager.class);

	VertexId myVertexId;
	Map<VertexId, VertexCausalLog> determinantLogs;
	DeterminantEncodingStrategy determinantEncodingStrategy;
	int numDownstreamChannels;

	public CausalLoggingManager(VertexGraphInformation vertexGraphInformation) {
		this(vertexGraphInformation.getThisTasksVertexId(), vertexGraphInformation.getUpstreamVertexes(), vertexGraphInformation.getNumberOfDirectDownstreamNeighbours());
	}

	public CausalLoggingManager(VertexId myVertexId, Collection<VertexId> upstreamVertexIds, int numDownstreamChannels) {
		LOG.debug("Creating new CausalLoggingManager for id {}, with upstreams {} and {} downstream channels", myVertexId, String.join(", ", upstreamVertexIds.stream().map(Object::toString).collect(Collectors.toList())), numDownstreamChannels);
		this.determinantLogs = new HashMap<>();
		this.myVertexId = myVertexId;

		for (VertexId u : upstreamVertexIds)
			determinantLogs.put(u, new CircularVertexCausalLog(numDownstreamChannels, u));
		this.determinantLogs.put(this.myVertexId, new CircularVertexCausalLog(numDownstreamChannels, this.myVertexId));

		this.determinantEncodingStrategy = new SimpleDeterminantEncodingStrategy();
		this.numDownstreamChannels = numDownstreamChannels;


	}


	@Override
	public List<VertexCausalLogDelta> getDeterminants() {
		List<VertexCausalLogDelta> results = new LinkedList<>();
		for (VertexId key : this.determinantLogs.keySet()) {
			results.add(new VertexCausalLogDelta(key, determinantLogs.get(key).getDeterminants(), 0));
		}
		results.add(new VertexCausalLogDelta(this.myVertexId, determinantLogs.get(this.myVertexId).getDeterminants(), 0));
		return results;
	}


	@Override
	public void appendDeterminant(Determinant determinant) {
		LOG.debug("Appending determinant {}", determinant);
		this.determinantLogs.get(this.myVertexId).appendDeterminants(
			this.determinantEncodingStrategy.encode(determinant)
		);
	}

	@Override
	public void processCausalLogDelta(VertexCausalLogDelta d) {
		LOG.debug("Processing UpstreamCausalLogDelta {}", d);
		LOG.debug("Determinant log pre processing: {}", this.determinantLogs.get(d.vertexId));
		this.determinantLogs.get(d.vertexId).processUpstreamVertexCausalLogDelta(d);
		LOG.debug("Determinant log post processing: {}", this.determinantLogs.get(d.vertexId));
	}

	@Override
	public void notifyCheckpointBarrier(long checkpointId) {
		LOG.debug("Processing checkpoint barrier {}", checkpointId);
		for (VertexCausalLog log : determinantLogs.values()) {
			LOG.debug("Determinant log pre processing: {}", log);
			log.notifyCheckpointBarrier(checkpointId);
			LOG.debug("Determinant log post processing: {}", log);
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
	public byte[] getDeterminantsOfVertex(VertexId vertexId) {
		return determinantLogs.get(vertexId).getDeterminants();
	}

	@Override
	public void enrichWithDeltas(DeterminantCarrier record, int targetChannel) {
		record.enrich(this.getNextDeterminantsForDownstream(targetChannel));
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		LOG.debug("Processing checkpoint complete notification for id {}", checkpointId);
		for (VertexCausalLog log : determinantLogs.values()) {
			LOG.debug("Determinant log pre processing: {}", log);
			log.notifyCheckpointComplete(checkpointId);
			LOG.debug("Determinant log post processing: {}", log);
		}
	}

	private List<VertexCausalLogDelta> getNextDeterminantsForDownstream(int channel) {
		LOG.debug("Getting deltas to send to downstream channel {}", channel);
		List<VertexCausalLogDelta> results = new LinkedList<>();
		for (VertexId key : this.determinantLogs.keySet()) {
			LOG.debug("Determinant log pre processing: {}", determinantLogs.get(key));
			VertexCausalLogDelta vertexCausalLogDelta = determinantLogs.get(key).getNextDeterminantsForDownstream(channel);
			if (vertexCausalLogDelta.rawDeterminants.length != 0)
				results.add(vertexCausalLogDelta);
			LOG.debug("Determinant log post processing: {}", determinantLogs.get(key));
		}
		return results;
	}

}
