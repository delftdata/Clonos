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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/*
Causal log for this operator. Contains Vertex Specific Causal logs for itself and all upstream operators.
 */
public class MapCausalLoggingManager implements CausalLoggingManager {

	private static final Logger LOG = LoggerFactory.getLogger(MapCausalLoggingManager.class);

	private Map<VertexId, VertexCausalLog> determinantLogs;
	private VertexId myVertexId;
	private DeterminantEncodingStrategy determinantEncodingStrategy;

	// Recovery fields
	private final int numDownstreamChannels;
	private int numProcessedDeterminantResponses;
	private CompletableFuture<Void> outputChannelConnectionsFuture;
	private ByteBuffer determinantsToRecoverFrom;
	private List<Silenceable> registeredSilenceables;
	private Determinant nextDeterminant;
	private boolean isRecovering;

	//services
	private final RandomService randomService;

	public MapCausalLoggingManager(VertexId myVertexId, Collection<VertexId> upstreamVertexIds, int numDownstreamChannels,  CompletableFuture<Void> unblockRecoveringTask) {
		LOG.info("Creating new CausalLoggingManager for id {}, with upstreams {} and {} downstream channels", myVertexId, String.join(", ", upstreamVertexIds.stream().map(Object::toString).collect(Collectors.toList())), numDownstreamChannels);
		this.determinantLogs = new HashMap<>();
		this.myVertexId = myVertexId;

		for (VertexId u : upstreamVertexIds)
			determinantLogs.put(u, new CircularVertexCausalLog(numDownstreamChannels, u));
		this.determinantLogs.put(this.myVertexId, new CircularVertexCausalLog(numDownstreamChannels, this.myVertexId));

		this.determinantEncodingStrategy = new SimpleDeterminantEncodingStrategy();

		this.outputChannelConnectionsFuture = unblockRecoveringTask;
		this.isRecovering = outputChannelConnectionsFuture != null;

		this.numDownstreamChannels = numDownstreamChannels;
		this.registeredSilenceables = new ArrayList<>(10);
		this.numProcessedDeterminantResponses = 0;
		this.determinantsToRecoverFrom = ByteBuffer.allocate(0);


		this.randomService = new RandomService(this);
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
	public void processCausalLogDelta(VertexCausalLogDelta d) {
		LOG.info("Processing UpstreamCausalLogDelta {}", d);
		LOG.info("Map entries: {}", String.join(",", determinantLogs.keySet().stream().map(Objects::toString).collect(Collectors.toList())));
		LOG.info("d.vertexId {}", d.vertexId);
		LOG.info("this.determinantLogs: {}", this.determinantLogs);
		LOG.info("this.determinantLogs.get(d.vertexId: {}", this.determinantLogs.get(d.vertexId));
		this.determinantLogs.get(d.vertexId).processUpstreamVertexCausalLogDelta(d);
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
	public void enrichWithDeltas(DeterminantCarrier record, int targetChannel) {
		record.enrich(this.getNextDeterminantsForDownstream(targetChannel));
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		for (VertexCausalLog log : determinantLogs.values())
			log.notifyCheckpointComplete(checkpointId);
	}

	private List<VertexCausalLogDelta> getNextDeterminantsForDownstream(int channel) {
		LOG.info("Getting deltas to send to downstream channel {}", channel);
		List<VertexCausalLogDelta> results = new LinkedList<>();
		for (VertexId key : this.determinantLogs.keySet()) {
			VertexCausalLogDelta vertexCausalLogDelta = determinantLogs.get(key).getNextDeterminantsForDownstream(channel);
			if (vertexCausalLogDelta.rawDeterminants.length != 0)
				results.add(vertexCausalLogDelta);
		}
		return results;
	}

	// =========== Recovery Manager =======================

	@Override
	public boolean isRecovering() {
		return isRecovering;
	}

	@Override
	public OrderDeterminant getRecoveryOrderDeterminant() {
		if(!(nextDeterminant instanceof OrderDeterminant))
			throw new RuntimeException("Unexpected determinant type! Expected Order but got " + nextDeterminant.getClass());

		OrderDeterminant toReturn = (OrderDeterminant) nextDeterminant;
		prepareNextRecoveryDeterminant();
		return toReturn;
	}

	@Override
	public RNGDeterminant getRecoveryRNGDeterminant() {
		if(!(nextDeterminant instanceof RNGDeterminant))
			throw new RuntimeException("Unexpected determinant type! Expected RNG but got " + nextDeterminant.getClass());
		RNGDeterminant toReturn = (RNGDeterminant) nextDeterminant;
		prepareNextRecoveryDeterminant();
		return (RNGDeterminant) toReturn;
	}

	private void prepareNextRecoveryDeterminant(){
		if(determinantsToRecoverFrom.hasRemaining())
			nextDeterminant = determinantEncodingStrategy.decodeNext(determinantsToRecoverFrom);

		while(nextDeterminant instanceof TimerTriggerDeterminant){
			processTimerDeterminant();
			if(determinantsToRecoverFrom.hasRemaining())
				nextDeterminant = determinantEncodingStrategy.decodeNext(determinantsToRecoverFrom);
		}

		if(nextDeterminant == null) { //recovery is finished
			isRecovering = false;
			unsilenceAll();
		}
	}


	private void processTimerDeterminant() {
		//todo
	}

	@Override
	public void notifyDeterminantResponseEvent(DeterminantResponseEvent determinantResponseEvent) {
		//Todo: possible optimization, after receiving the first response we could immediately start recovery
		numProcessedDeterminantResponses++;

		//If this downstream has a more up to date causal log, we use it for recovery.
		if(determinantResponseEvent.getVertexCausalLogDelta().rawDeterminants.length > determinantsToRecoverFrom.array().length)
			determinantsToRecoverFrom = ByteBuffer.wrap(determinantResponseEvent.getVertexCausalLogDelta().rawDeterminants);

		if(numProcessedDeterminantResponses == numDownstreamChannels) {//Received all responses, Unblock the task
			isRecovering = true;
			silenceAll();
			prepareNextRecoveryDeterminant();
			outputChannelConnectionsFuture.complete(null);
		}
	}

	@Override
	public void registerSilenceable(Silenceable silenceable) {
		this.registeredSilenceables.add(silenceable);
	}

	private void silenceAll(){
		for(Silenceable s: registeredSilenceables)
			s.silence();
	}

	private void unsilenceAll(){
		for(Silenceable s: registeredSilenceables)
			s.unsilence();
	}

	//============= Services ===================

	@Override
	public RandomService getRandomService() {
		return randomService;
	}
}
