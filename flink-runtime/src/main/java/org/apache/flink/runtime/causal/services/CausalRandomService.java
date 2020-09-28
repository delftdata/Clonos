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

import org.apache.flink.api.common.services.RandomService;
import org.apache.flink.runtime.causal.EpochProvider;
import org.apache.flink.runtime.causal.log.job.IJobCausalLog;
import org.apache.flink.runtime.causal.determinant.RNGDeterminant;
import org.apache.flink.runtime.causal.recovery.IRecoveryManager;
import org.apache.flink.util.XORShiftRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CausalRandomService extends AbstractCausalService implements RandomService {


	//Not thread safe
	protected final XORShiftRandom rng = new XORShiftRandom();

	//RNG determinant object used to avoid object creation and so garbage collection
	private RNGDeterminant reuseRNGDeterminant;

	private static final Logger LOG = LoggerFactory.getLogger(CausalRandomService.class);

	public CausalRandomService(IJobCausalLog causalLoggingManager, IRecoveryManager recoveryManager,
							   EpochProvider epochProvider) {
		super(recoveryManager, causalLoggingManager, epochProvider);
		this.reuseRNGDeterminant = new RNGDeterminant();
	}

	@Override
	public int nextInt() {
		return this.nextInt(Integer.MAX_VALUE);
	}

	@Override
	public int nextInt(int maxExclusive) {
		int toReturn;

		if (isRecovering()) {
			toReturn = recoveryManager.replayRandomInt();
			if (LOG.isDebugEnabled())
				LOG.info("nextInt(): (State: RECOVERING) Replayed random is {}", toReturn);
		} else {
			toReturn = rng.nextInt(maxExclusive);
			if (LOG.isDebugEnabled())
				LOG.info("nextInt(): (State: RUNNING) Fresh random is {}", toReturn);
		}

		//Whether we are recovering or not, we append the determinant. If recovering, we still need to restore the
		// causal log to the pre-failure state.
		causalLoggingManager.appendDeterminant(reuseRNGDeterminant.replace(toReturn),
			epochProvider.getCurrentEpochID());
		return toReturn;
	}

}
