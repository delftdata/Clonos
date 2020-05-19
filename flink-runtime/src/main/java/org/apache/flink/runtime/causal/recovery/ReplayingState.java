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

package org.apache.flink.runtime.causal.recovery;

import org.apache.flink.runtime.causal.determinant.*;
import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;

/**
 * In this state we do the actual process of recovery. Once done transition to {@link RunningState}
 *
 * A downstream failure in this state does not matter, requests simply get added to the queue of unanswered requests.
 * Only when we finish our recovery do we answer those requests.
 *
 * Upstream failures in this state mean that perhaps the upstream will not have sent all buffers. This means we must resend
 * replay requests.
 */
public class ReplayingState extends AbstractState {

	ByteBuffer recoveryBuffer;
	DeterminantEncodingStrategy determinantEncodingStrategy;
	Determinant nextDeterminant;
	Queue<NonMainThreadDeterminant> toProcess;
	private boolean isDone;

	public ReplayingState(RecoveryManager context, byte[] determinantsToRecoverFrom) {
		super(context);

		determinantEncodingStrategy = context.jobCausalLoggingManager.getDeterminantEncodingStrategy();
		recoveryBuffer = ByteBuffer.wrap(determinantsToRecoverFrom);

		toProcess = new LinkedList<>();
		isDone = false;
		fillQueue();

		context.readyToReplayFuture.complete(null);//allow task to start running
	}

	@Override
	public void notifyNewInputChannel(InputGate gate, int channelIndex, int numberOfBuffersRemoved){
		//we got notified of a new input channel while we were replaying
		//This means that  we now have to wait for the upstream to finish recovering before we do.
		//Furthermore, we have to resend the inflight log request, and ask to skip X buffers
		IntermediateDataSetID intermediateDataSetID = ((SingleInputGate)gate).getConsumedResultId();
		int subpartitionIndex = ((SingleInputGate) gate).getConsumedSubpartitionIndex();

		try {
			((RemoteInputChannel)gate.getInputChannel(channelIndex)).sendTaskEvent(new InFlightLogRequestEvent(intermediateDataSetID, subpartitionIndex, context.finalRestoredCheckpointId, numberOfBuffersRemoved));
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		LOG.info("Got notified of unexpected NewChannel event, while in state " + this.getClass());
	}


	//===================

	@Override
	public int replayRandomInt() {
		drainQueue();
		if(!(nextDeterminant instanceof RNGDeterminant))
			throw new RuntimeException("Unexpected Determinant type: Expected RNG, but got: " + nextDeterminant);

		int toReturn = ((RNGDeterminant) nextDeterminant).getNumber();
		fillQueue();
		return toReturn;
	}

	@Override
	public byte replayNextChannel() {
		drainQueue();
		if(!(nextDeterminant instanceof OrderDeterminant))
			throw new RuntimeException("Unexpected Determinant type: Expected Order, but got: " + nextDeterminant);

		byte toReturn = ((OrderDeterminant) nextDeterminant).getChannel();
		fillQueue();
		return toReturn;
	}

	@Override
	public long replayNextTimestamp() {
		drainQueue();
		if(!(nextDeterminant instanceof TimestampDeterminant))
			throw new RuntimeException("Unexpected Determinant type: Expected Timestamp, but got: " + nextDeterminant);

		long toReturn = ((TimestampDeterminant) nextDeterminant).getTimestamp();
		fillQueue();
		return toReturn;
	}

	private void drainQueue(){
		while(!toProcess.isEmpty()) {
			LOG.info("Processing off main thread determinant {}", toProcess.peek());
			toProcess.poll().process(context);
		}
	}

	private void fillQueue(){
		nextDeterminant = determinantEncodingStrategy.decodeNext(recoveryBuffer);
		//We must synchronously process the non main thread determinants
		while (nextDeterminant instanceof NonMainThreadDeterminant) {
			toProcess.add((NonMainThreadDeterminant) nextDeterminant);
			nextDeterminant = determinantEncodingStrategy.decodeNext(recoveryBuffer);
		}

		if(nextDeterminant == null) {
			//we cant immediately drain the queue. We need to return the determinant first, then process it and end the replay
			isDone = true;

		}
	}

	public boolean isDone(){
		return isDone;
	}

	public void finish(){
		drainQueue();
		context.setState(new RunningState(context));
	}

	@Override
	public String toString() {
		return "ReplayingState{}";
	}
}
