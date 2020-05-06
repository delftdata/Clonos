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

import java.nio.ByteBuffer;

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

	public ReplayingState(RecoveryManager context, byte[] determinantsToRecoverFrom) {
		super(context);
		context.readyToReplayFuture.complete(null);//allow task to start running

		determinantEncodingStrategy = context.causalLoggingManager.getDeterminantEncodingStrategy();
		recoveryBuffer = ByteBuffer.wrap(determinantsToRecoverFrom);

		if(determinantsToRecoverFrom.length == 0)
			context.setState(new RunningState(context));

		nextDeterminant = determinantEncodingStrategy.decodeNext(recoveryBuffer);
		processTimerDeterminants();
	}

	private void processTimerDeterminants() {
		while (nextDeterminant instanceof TimerTriggerDeterminant) {
			//todo do something

			prepareNextDeterminant();
		}
	}

	//===================

	@Override
	public int replayRandomInt() {
		if(!(nextDeterminant instanceof RNGDeterminant))
			throw new RuntimeException("Unexpected Determinant type: Expected RNG, but got: " + nextDeterminant);

		int toReturn = ((RNGDeterminant) nextDeterminant).getNumber();
		prepareNextDeterminant();
		return toReturn;
	}

	@Override
	public byte replayNextChannel() {
		if(!(nextDeterminant instanceof OrderDeterminant))
			throw new RuntimeException("Unexpected Determinant type: Expected Order, but got: " + nextDeterminant);

		byte toReturn = ((OrderDeterminant) nextDeterminant).getChannel();
		prepareNextDeterminant();
		return toReturn;
	}

	@Override
	public long replayNextTimestamp() {
		if(!(nextDeterminant instanceof TimestampDeterminant))
			throw new RuntimeException("Unexpected Determinant type: Expected Timestamp, but got: " + nextDeterminant);

		long toReturn = ((TimestampDeterminant) nextDeterminant).getTimestamp();
		prepareNextDeterminant();
		return toReturn;
	}

	private void prepareNextDeterminant(){
		nextDeterminant = determinantEncodingStrategy.decodeNext(recoveryBuffer);
		if(nextDeterminant == null)
			context.setState(new RunningState(context));
	}

	@Override
	public String toString() {
		return "ReplayingState{}";
	}
}
