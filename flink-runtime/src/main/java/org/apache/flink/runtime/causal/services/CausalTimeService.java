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

import org.apache.flink.api.common.services.TimeService;
import org.apache.flink.runtime.causal.EpochProvider;
import org.apache.flink.runtime.causal.log.job.IJobCausalLog;
import org.apache.flink.runtime.causal.determinant.TimestampDeterminant;
import org.apache.flink.runtime.causal.recovery.IRecoveryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CausalTimeService implements TimeService {

	private final EpochProvider epochProvider;
	private IJobCausalLog causalLoggingManager;
	private IRecoveryManager recoveryManager;
	private final TimestampDeterminant reuseTimestampDeterminant;

	private static final Logger LOG = LoggerFactory.getLogger(CausalTimeService.class);

	public CausalTimeService(IJobCausalLog causalLoggingManager, IRecoveryManager recoveryManager, EpochProvider epochProvider){
		this.causalLoggingManager = causalLoggingManager;
		this.recoveryManager = recoveryManager;
		this.epochProvider = epochProvider;
		this.reuseTimestampDeterminant = new TimestampDeterminant();
	}

	@Override
	public long currentTimeMillis(){
		while (!(recoveryManager.isRunning() || recoveryManager.isReplaying()))
			LOG.debug("Requested timestamp but neither running nor replaying");


		long toReturn;
		if(recoveryManager.isReplaying())
			toReturn = recoveryManager.replayNextTimestamp();
		else
			toReturn = System.currentTimeMillis();

		LOG.debug("Appending time determinant");
		causalLoggingManager.appendDeterminant(reuseTimestampDeterminant.replace(toReturn), epochProvider.getCurrentEpochID());
		LOG.debug("We are running, returning a fresh timestamp and recording it.");
		return toReturn;
	}
}
