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

import org.apache.flink.runtime.causal.ICausalLoggingManager;
import org.apache.flink.runtime.causal.determinant.TimestampDeterminant;
import org.apache.flink.runtime.causal.recovery.IRecoveryManager;

public class TimeService {

	private ICausalLoggingManager causalLoggingManager;
	private IRecoveryManager recoveryManager;

	public TimeService(ICausalLoggingManager causalLoggingManager, IRecoveryManager recoveryManager){
		this.causalLoggingManager = causalLoggingManager;
		this.recoveryManager = recoveryManager;
	}

	public long currentTimeMillis(){
		while (!(recoveryManager.isRunning() || recoveryManager.isReplaying())); //Spin

		if(recoveryManager.isReplaying())
			return  recoveryManager.replayNextTimestamp();

		long timestamp = System.currentTimeMillis();
		causalLoggingManager.appendDeterminant(new TimestampDeterminant(timestamp));
		return timestamp;
	}



}
