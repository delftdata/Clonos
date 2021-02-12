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

import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public abstract class AbstractState implements State {

	protected static final Logger LOG = LoggerFactory.getLogger(AbstractState.class);


	protected final RecoveryManager recoveryManager;
	protected final RecoveryManagerContext context;

	protected final Random random;


	public AbstractState(RecoveryManager recoveryManager, RecoveryManagerContext context) {
		this.recoveryManager = recoveryManager;
		this.context = context;
		this.random = new Random(System.currentTimeMillis());
	}


	@Override
	public void notifyNewInputChannel(InputChannel remoteInputChannel, int consumedSubpartitionIndex,
									  int numBuffersRemoved) {
		//we got notified of a new input channel while we were recovering.
		//This means that  we now have to wait for the upstream to finish recovering before we do.
		//Furthermore, if we have already sent an inflight log request for this channel, we now have to send it again.
		logInfoWithVertexID("Got notified of unexpected NewInputChannel event, while in state " + this.getClass());
	}

	@Override
	public void notifyNewOutputChannel(IntermediateResultPartitionID intermediateResultPartitionID,
									   int subpartitionIndex) {
		logInfoWithVertexID("Got notified of unexpected NewOutputChannel event, while in state " + this.getClass());

	}

	@Override
	public void notifyInFlightLogRequestEvent(InFlightLogRequestEvent e) {
		logInfoWithVertexID("Received request: {}", e);
		//we got an inflight log request while still recovering. Since we must finish recovery first before
		//answering, we store it, and when we enter the running state we immediately process it.
		context.unansweredInFlightLogRequests.put(e.getIntermediateResultPartitionID(), e.getSubpartitionIndex(), e);
	}

	@Override
	public void notifyStateRestorationStart(long checkpointId) {
		logInfoWithVertexID("Started restoring state of checkpoint {}", checkpointId);
		this.context.incompleteStateRestorations.add(checkpointId);
		//if (checkpointId > context.epochTracker.getCurrentEpoch())
		//	context.getEpochTracker().startNewEpoch(checkpointId); //TODO this will notify subscribers, which we dont want

		//for (PipelinedSubpartition ps : context.subpartitionTable.values())
		//	ps.setStartingEpoch(context.getEpochTracker().getCurrentEpoch());
	}

	@Override
	public void notifyStateRestorationComplete(long checkpointId) {
		logInfoWithVertexID("Completed restoring state of checkpoint {}", checkpointId);
		this.context.incompleteStateRestorations.remove(checkpointId);
	}


	@Override
	public void notifyStartRecovery() {
		LOG.info("Unexpected notification StartRecovery in state " + this.getClass());
	}


	/**
	 * Simple utility method for prepending vertex id to a log message
	 */
	protected void logDebugWithVertexID(String s, Object... a) {
			List<Object> array = new ArrayList<>(a.length + 1);
			array.add(context.getTaskVertexID());
			array.addAll(Arrays.asList(a));
			LOG.debug("Vertex {} - " + s, array.toArray());
	}

	/**
	 * Simple utility method for prepending vertex id to a log message
	 */
	protected void logInfoWithVertexID(String s, Object... a) {
		List<Object> array = new ArrayList<>(a.length + 1);
		array.add(context.getTaskVertexID());
		array.addAll(Arrays.asList(a));
		LOG.info("Vertex {} - " + s, array.toArray());
	}

}
