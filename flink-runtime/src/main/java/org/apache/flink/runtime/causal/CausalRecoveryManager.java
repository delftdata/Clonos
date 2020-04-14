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

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.runtime.causal.determinant.Determinant;
import org.apache.flink.runtime.causal.determinant.DeterminantEncodingStrategy;
import org.apache.flink.runtime.causal.determinant.OrderDeterminant;
import org.apache.flink.runtime.causal.determinant.TimerTriggerDeterminant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Manages a causal recovery process.
 * Processes the {@link DeterminantResponseEvent} to stitch together the complete determinant history.
 * When the determinant history is obtained, it unblocks the standby execution, who will start consuming replay records
 * It can deserialize determinants one by one, maintaining low resource usage.
 *
 * One other important feature is that upon encountering a "TimerTriggerDeterminant", it will transparently cause the
 * appropriate timer to trigger.
 *
 */
public class CausalRecoveryManager implements RecoveryManager {
	private static final Logger LOG = LoggerFactory.getLogger(CausalRecoveryManager.class);

	private final DeterminantEncodingStrategy determinantEncodingStrategy;

	private int numDownstreamChannels;

	private int numProcessedDeterminantResponses;

	private CompletableFuture<Void> outputChannelConnectionsFuture;

	private ByteBuffer determinantsToRecoverFrom;

	private List<Silenceable> registeredSilenceables;

	private Determinant nextDeterminant;

	public CausalRecoveryManager(int numDownstreamChannels, DeterminantEncodingStrategy determinantEncodingStrategy) {
		this.numDownstreamChannels = numDownstreamChannels;
		this.determinantEncodingStrategy = determinantEncodingStrategy;
		this.registeredSilenceables = new ArrayList<Silenceable>(10);
		reset();
	}

	private void reset() {
		this.numProcessedDeterminantResponses = 0;
		this.determinantsToRecoverFrom = ByteBuffer.allocate(0);
	}

	@Override
	public boolean isRecovering() {
		return numDownstreamChannels == numProcessedDeterminantResponses;
	}

	@Override
	public OrderDeterminant getNextOrderDeterminant() {
		if(!(nextDeterminant instanceof OrderDeterminant))
			throw new RuntimeException("Unexpected determinant type! Expected Order but got " + nextDeterminant.getClass());

		getNextDeterminant();
		return (OrderDeterminant) nextDeterminant;
	}

	private void getNextDeterminant(){
		nextDeterminant = determinantEncodingStrategy.decodeNext(determinantsToRecoverFrom);

		while(nextDeterminant instanceof TimerTriggerDeterminant){
			processTimerDeterminant();
			nextDeterminant = determinantEncodingStrategy.decodeNext(determinantsToRecoverFrom);
		}

		if(nextDeterminant == null) //recovery is finished
			reset();
	}


	private void processTimerDeterminant() {
		//todo
	}

	@Override
	public void notifyDeterminantResponseEvent(DeterminantResponseEvent determinantResponseEvent) {
		numProcessedDeterminantResponses++;

		//If this downstream has a more up to date causal log, we use it for recovery.
		if(determinantResponseEvent.getVertexCausalLogDelta().rawDeterminants.length > determinantsToRecoverFrom.array().length)
			determinantsToRecoverFrom = ByteBuffer.wrap(determinantResponseEvent.getVertexCausalLogDelta().rawDeterminants);

		if(numProcessedDeterminantResponses == numDownstreamChannels) {//Received all responses, Unblock the task
			getNextDeterminant();
			outputChannelConnectionsFuture.complete(null);
		}
	}

	@Override
	public void registerOutputChannelConnectionsFuture(CompletableFuture<Void> future) {
		this.outputChannelConnectionsFuture = future;
	}

	@Override
	public void registerSilenceable(Silenceable silenceable) {
		LOG.info("Registering a new Silenceable");
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



}
