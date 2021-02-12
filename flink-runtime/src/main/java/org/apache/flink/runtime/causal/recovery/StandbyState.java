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

import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * In this state, we are waiting for recovery to begin.
 * We may receive and process restoreState notifications.
 * When notified of recovery start, we switch to {@link WaitingConnectionsState}
 * where we will wait for all connections to be established.
 */
public class StandbyState extends AbstractState {
	private static final Logger LOG = LoggerFactory.getLogger(StandbyState.class);

	//Concurrent sets with notifications received before the WaitConnectionsState
	//These may happen when we do not use highly available standby tasks
	private final Set<EarlyNewInputChannelNotification> inputChannelNotifications;
	private final Set<EarlyNewOutputChannelNotification> outputChannelNotifications;

	public StandbyState(RecoveryManager recoveryManager, RecoveryManagerContext context) {
		super(recoveryManager, context);

		this.inputChannelNotifications = new HashSet<>();
		this.outputChannelNotifications = new HashSet<>();
	}

	@Override
	public void executeEnter() {

	}

	@Override
	public void notifyStartRecovery() {
		logInfoWithVertexID("Received start recovery notification!");

		State newState = new WaitingConnectionsState(recoveryManager, context);
		recoveryManager.setState(newState);

		// Notify state of save notifications
		for (EarlyNewInputChannelNotification i : inputChannelNotifications) {
			newState.notifyNewInputChannel(i.getInputChannel(), i.getConsumedSubpartitionIndex(),
				i.getNumBuffersRemoved());
		}
		for (EarlyNewOutputChannelNotification o : outputChannelNotifications) {
			newState.notifyNewOutputChannel(o.getIntermediateResultPartitionID(), o.subpartitionIndex);
		}
	}

	@Override
	public void notifyNewInputChannel(InputChannel remoteInputChannel, int consumedSubpartitionIndex,
									  int numBuffersRemoved) {
		this.inputChannelNotifications.add(new EarlyNewInputChannelNotification(remoteInputChannel,
			consumedSubpartitionIndex, numBuffersRemoved));

	}

	@Override
	public void notifyNewOutputChannel(IntermediateResultPartitionID intermediateResultPartitionID,
									   int subpartitionIndex) {
		this.outputChannelNotifications.add(new EarlyNewOutputChannelNotification(intermediateResultPartitionID,
			subpartitionIndex));
	}

	@Override
	public String toString() {
		return "StandbyState{}";
	}

	// ========== Notification parameter storage
	private static class EarlyNewOutputChannelNotification {
		private final IntermediateResultPartitionID intermediateResultPartitionID;
		private final int subpartitionIndex;

		public EarlyNewOutputChannelNotification(IntermediateResultPartitionID intermediateResultPartitionID,
												 int subpartitionIndex) {
			this.intermediateResultPartitionID = intermediateResultPartitionID;
			this.subpartitionIndex = subpartitionIndex;
		}

		public IntermediateResultPartitionID getIntermediateResultPartitionID() {
			return intermediateResultPartitionID;
		}

		public int getSubpartitionIndex() {
			return subpartitionIndex;
		}
	}

	private static class EarlyNewInputChannelNotification {
		private final InputChannel inputChannel;
		private final int consumedSubpartitionIndex;
		private final int numBuffersRemoved;

		public EarlyNewInputChannelNotification(InputChannel inputChannel, int consumedSubpartitionIndex,
												int numBuffersRemoved) {
			this.inputChannel = inputChannel;
			this.consumedSubpartitionIndex = consumedSubpartitionIndex;
			this.numBuffersRemoved = numBuffersRemoved;
		}

		public InputChannel getInputChannel() {
			return inputChannel;
		}

		public int getConsumedSubpartitionIndex() {
			return consumedSubpartitionIndex;
		}

		public int getNumBuffersRemoved() {
			return numBuffersRemoved;
		}
	}

}
