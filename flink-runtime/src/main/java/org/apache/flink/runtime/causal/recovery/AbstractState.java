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

import com.esotericsoftware.minlog.Log;
import org.apache.flink.runtime.causal.DeterminantResponseEvent;
import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractState implements State {

	protected static final Logger LOG = LoggerFactory.getLogger(AbstractState.class);
	protected final RecoveryManager context;


	public AbstractState(RecoveryManager context){
		this.context = context;
	}


	@Override
	public void notifyNewChannel(InputGate gate, int channelIndex){
		LOG.info("Unexpected notification NewChannel in state " + this.getClass());

	}

	@Override
	public void notifyInFlightLogRequestEvent(InFlightLogRequestEvent e) {
		LOG.info("Unexpected notification InFlightLogRequest in state " + this.getClass());
	}

	@Override
	public void notifyStateRestorationStart(long checkpointId) {
		LOG.info("Started restoring state of checkpoint {}", checkpointId);
		this.context.incompleteStateRestorations.put(checkpointId, true);
		if(checkpointId > context.finalRestoredCheckpointId)
			context.finalRestoredCheckpointId = checkpointId;
		//throw new RuntimeException("Unexpected notification StateRestorationStart in state " + this.getClass());

	}

	@Override
	public void notifyStateRestorationComplete(long checkpointId) {
		LOG.info("Completed restoring state of checkpoint {}", checkpointId);
		this.context.incompleteStateRestorations.remove(checkpointId);
		//throw new RuntimeException("Unexpected notification StateRestorationComplete in state " + this.getClass());
	}

	@Override
	public void notifyDeterminantResponseEvent(DeterminantResponseEvent e) {
		LOG.info("Received a DeterminantResponseEvent, but am not recovering: {}", e);
		//throw new RuntimeException("Unexpected notification DeterminantResponseEvent in state " + this.getClass());

	}

	@Override
	public void notifyDeterminantRequestEvent(DeterminantRequestEvent e,int channelRequestArrivedFrom) {
		LOG.info("Unexpected notification DeterminantRequestEvent in state " + this.getClass());
	}

	@Override
	public void notifyStartRecovery() {
		LOG.info("Unexpected notification StartRecovery in state " + this.getClass());
	}

	//==============================================================

	@Override
	public int replayRandomInt() {
		throw new RuntimeException("Unexpected replayRandomInt request in state " + this.getClass());
	}

	@Override
	public byte replayNextChannel() {
		throw new RuntimeException("Unexpected replayNextChannel request in state " + this.getClass());
	}

	@Override
	public long replayNextTimestamp() {
		throw new RuntimeException("Unexpected replayNextTimestamp request in state " + this.getClass());
	}
}
