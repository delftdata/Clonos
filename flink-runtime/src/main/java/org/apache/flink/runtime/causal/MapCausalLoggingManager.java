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

import org.apache.flink.runtime.causal.determinant.Determinant;
import org.apache.flink.runtime.causal.determinant.DeterminantEncodingStrategy;
import org.apache.flink.runtime.causal.determinant.SimpleDeterminantEncodingStrategy;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/*
Causal log for this operator. Contains Vertex Specific Causal logs for itself and all upstream operators.
 */
public class MapCausalLoggingManager implements CausalLoggingManager {

	private static final Logger LOG = LoggerFactory.getLogger(MapCausalLoggingManager.class);

	private Map<VertexId, VertexCausalLog> determinantLogs;
	private VertexId myVertexId;
	private DeterminantEncodingStrategy determinantEncodingStrategy;
	private List<Silenceable> registeredSilenceables;
	private RecoveryManager recoveryManager;


	public MapCausalLoggingManager(VertexId myVertexId, Collection<VertexId> upstreamVertexIds, int numDownstreamChannels) {
		LOG.info("Creating new CausalLoggingManager for id {}, with upstreams {} and {} downstream channels", myVertexId, String.join(", ", upstreamVertexIds.stream().map(Object::toString).collect(Collectors.toList())), numDownstreamChannels);
		this.determinantLogs = new HashMap<>();
		this.myVertexId = myVertexId;
		for (VertexId u : upstreamVertexIds)
			determinantLogs.put(u, new CircularVertexCausalLog(numDownstreamChannels, u));
		this.determinantLogs.put(this.myVertexId, new CircularVertexCausalLog(numDownstreamChannels, this.myVertexId));
		this.determinantEncodingStrategy = new SimpleDeterminantEncodingStrategy();
		this.registeredSilenceables = new ArrayList<Silenceable>(10);

		recoveryManager = new RecoveryManager(numDownstreamChannels, determinantEncodingStrategy);
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
		LOG.info("Appending determinant {}", determinant);
		this.determinantLogs.get(this.myVertexId).appendDeterminants(
			this.determinantEncodingStrategy.encode(determinant)
		);
	}

	@Override
	public void processUpstreamCausalLogDelta(VertexCausalLogDelta d) {
		LOG.info("Processing UpstreamCausalLogDelta {}", d);
		LOG.info("Map entries: {}", String.join(",", determinantLogs.keySet().stream().map(Objects::toString).collect(Collectors.toList())));
		LOG.info("d.vertexId {}", d.vertexId);
		LOG.info("this.determinantLogs: {}", this.determinantLogs);
		LOG.info("this.determinantLogs.get(d.vertexId: {}", this.determinantLogs.get(d.vertexId));
		this.determinantLogs.get(d.vertexId).processUpstreamVertexCausalLogDelta(d);
	}

	@Override
	public List<VertexCausalLogDelta> getNextDeterminantsForDownstream(int channel) {
		LOG.info("Getting deltas to send to downstream channel {}", channel);
		List<VertexCausalLogDelta> results = new LinkedList<>();
		for (VertexId key : this.determinantLogs.keySet()) {
			VertexCausalLogDelta vertexCausalLogDelta = determinantLogs.get(key).getNextDeterminantsForDownstream(channel);
			if (vertexCausalLogDelta.rawDeterminants.length != 0)
				results.add(vertexCausalLogDelta);
		}
		return results;
	}

	@Override
	public void notifyCheckpointBarrier(long checkpointId) {
		for (VertexCausalLog log : determinantLogs.values())
			log.notifyCheckpointBarrier(checkpointId);
	}

	@Override
	public void notifyDownstreamFailure(int channel) {
		for (VertexCausalLog log : determinantLogs.values())
			log.notifyDownstreamFailure(channel);
	}

	@Override
	public byte[] getDeterminantsOfVertex(VertexId vertexId) {
		return determinantLogs.get(vertexId).getDeterminants();
	}


	@Override
	public void registerSilenceable(Silenceable silenceable) {
		LOG.info("Registering a new Silenceable");
		this.registeredSilenceables.add(silenceable);
	}

	@Override
	public void silenceAll() {
		LOG.info("Silencing all Silenceables");
		for (Silenceable s : this.registeredSilenceables)
			s.silence();
	}

	@Override
	public void unsilenceAll() {
		LOG.info("UNSilencing all Silenceables");
		for (Silenceable s : this.registeredSilenceables)
			s.unsilence();

	}

	@Override
	public <T> void enrichWithDeltas(T record, int targetChannel) {
		LOG.info("Call to enrich with deltas");
		SerializationDelegate<LogDeltaCarryingStreamElement> r = (SerializationDelegate<LogDeltaCarryingStreamElement>) record;
		r.getInstance().setLogDeltas(this.getNextDeterminantsForDownstream(targetChannel));
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		for (VertexCausalLog log : determinantLogs.values())
			log.notifyCheckpointComplete(checkpointId);
	}

	@Override
	public RecoveryManager getRecoveryManager() {
		return recoveryManager;
	}


}
