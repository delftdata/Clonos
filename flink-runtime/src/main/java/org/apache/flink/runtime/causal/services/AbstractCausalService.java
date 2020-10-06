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

package org.apache.flink.runtime.causal.services;

import org.apache.flink.runtime.causal.EpochProvider;
import org.apache.flink.runtime.causal.log.job.CausalLogID;
import org.apache.flink.runtime.causal.log.job.JobCausalLog;
import org.apache.flink.runtime.causal.log.thread.ThreadCausalLog;
import org.apache.flink.runtime.causal.recovery.IRecoveryManager;

/**
 * Causal services have a general structure.
 * Whenever a request is made to one, we first have to check whether we are recovering.
 * If we are not, a new nondeterministic event occurs, which must be recorded into the causal log.
 */
public abstract class AbstractCausalService {

	// Causal services will request replay of a single nondeterministic event from the recovery manager
	protected final IRecoveryManager recoveryManager;

	// Causal services will append the determinant of a single nondeterministic event to the main thread log
	protected final ThreadCausalLog threadCausalLog;

	// Causal services need to know to which epoch this nondeterministic event belongs to
	protected final EpochProvider epochProvider;

	// Boolean used to short-circuit accesses to the recovery manager when not in recovery
	private boolean isRecovering;

	public AbstractCausalService(JobCausalLog causalLog, IRecoveryManager recoveryManager,
								 EpochProvider epochProvider){
		CausalLogID causalLogID = new CausalLogID(recoveryManager.getTaskVertexID().getVertexID());
		this.threadCausalLog = causalLog.getThreadCausalLog(causalLogID);
		this.recoveryManager = recoveryManager;
		this.epochProvider = epochProvider;
		this.isRecovering = !recoveryManager.isRunning();
	}

	/**
	 * This method informs the causal service of whether it should fetch a causally recovered version of the determinant
	 * or just create a new one.
	 * We extract this method so that a short circuit logic can be abstracted away, leading to better performance by
	 * avoiding consulting the recovery manager when in RunningState.
	 * We can do this because RunningState is a sort of attractor, when we go into RunningState we never return to any
	 * previous state.
	 */
	protected boolean isRecovering(){
		return isRecovering && (isRecovering = !recoveryManager.isRunning());
	}

}
