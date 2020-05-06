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

import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class WaitingConnectionsState extends AbstractState{

	private static final Logger LOG = LoggerFactory.getLogger(WaitingConnectionsState.class);
	Boolean[] channelsReestablishmentStatus;

	public WaitingConnectionsState(RecoveryManager context) {
		super(context);
		channelsReestablishmentStatus = new Boolean[context.inputGate.getNumberOfInputChannels()];
		LOG.info("Waiting for new connections!");
	}


	@Override
	public void notifyNewChannel(InputGate gate, int channelIndex) {
		LOG.info("Got Notified of new channel for gate {}, index {}. New channelsReestablishmentStatus={}", gate, channelIndex, Arrays.toString(channelsReestablishmentStatus));
		channelsReestablishmentStatus[context.inputGate.getAbsoluteChannelIndex(gate, channelIndex)] = true;

		if(Arrays.stream(channelsReestablishmentStatus).allMatch(x -> x)){
			State newState = new WaitingDeterminantsState(context);
			context.setState(newState);
		}
	}

	@Override
	public String toString() {
		return "WaitingConnectionsState{" +
			"channelsReestablishmentStatus=" + Arrays.toString(channelsReestablishmentStatus) +
			'}';
	}
}
