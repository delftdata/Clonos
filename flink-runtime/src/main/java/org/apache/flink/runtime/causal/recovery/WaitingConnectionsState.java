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

import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Stream;

public class WaitingConnectionsState extends AbstractState{

	private static final Logger LOG = LoggerFactory.getLogger(WaitingConnectionsState.class);

	Boolean[] inputChannelsReestablishmentStatus;
	Map<IntermediateResultPartitionID, Boolean[]> outputChannelsReestablishmentStatus;


	public WaitingConnectionsState(RecoveryManager recoveryManager, RecoveryManagerContext context) {
		super(recoveryManager,context);

		inputChannelsReestablishmentStatus = new Boolean[0];
		outputChannelsReestablishmentStatus = new HashMap<>();

		if(context.vertexGraphInformation.hasUpstream()) {
			inputChannelsReestablishmentStatus = new Boolean[context.inputGate.getNumberOfInputChannels()];
			Arrays.fill(inputChannelsReestablishmentStatus, Boolean.FALSE);
		}

		if(context.vertexGraphInformation.hasDownstream()) {
			for (IntermediateResultPartitionID partID : context.subpartitionTable.rowKeySet()) {
				Boolean[] array = new Boolean[context.subpartitionTable.row(partID).size()];
				Arrays.fill(array, Boolean.FALSE);
				outputChannelsReestablishmentStatus.put(partID, array);
			}
		}

		logInfoWithVertexID("Waiting for new connections!");
	}


	@Override
	public void executeEnter() {

	}

	@Override
	public void notifyNewInputChannel(InputChannel inputChannel, int consumedSubpartitionIndex, int numberOfBuffersRemoved) {
		logInfoWithVertexID("Got Notified of new input channel {}, consuming index {} and having to skip {} buffers.", inputChannel, consumedSubpartitionIndex, numberOfBuffersRemoved);
		SingleInputGate singleInputGate = inputChannel.getInputGate();
		int channelIndex = inputChannel.getChannelIndex();
		inputChannelsReestablishmentStatus[context.inputGate.getAbsoluteChannelIndex(singleInputGate, channelIndex)] = Boolean.TRUE;
		checkConnectionsComplete();
	}


	@Override
	public void notifyNewOutputChannel(IntermediateResultPartitionID intermediateResultPartitionID, int subpartitionIndex){
		logInfoWithVertexID("Got Notified of new output channel for intermediateResultPartition {} index {}.", intermediateResultPartitionID, subpartitionIndex);
		outputChannelsReestablishmentStatus.get(intermediateResultPartitionID)[subpartitionIndex] = true;
		checkConnectionsComplete();
	}

	private void checkConnectionsComplete() {
		Stream<Boolean> channelStatus = Arrays.stream(inputChannelsReestablishmentStatus);
		for(Boolean[] booleans : outputChannelsReestablishmentStatus.values())
			channelStatus = Stream.concat(channelStatus, Arrays.stream(booleans));
		if(channelStatus.allMatch(x -> x)){
			logInfoWithVertexID("Got all connections set-up. Switching to WaitingDeterminantsState.");
			State newState = new ReplayingState(recoveryManager,context);
			recoveryManager.setState(newState);
		}
	}

	@Override
	public String toString() {
		return "WaitingConnectionsState{" +
			"channelsReestablishmentStatus=" + Arrays.toString(inputChannelsReestablishmentStatus) +
			'}';
	}
}
