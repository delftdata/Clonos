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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In this state, we are waiting for recovery to begin.
 * We may receive and process restoreState notifications.
 * When notified of recovery start, we switch to {@link WaitingConnectionsState}
 * where we will wait for all connections to be established.
 *
 */
public class StandbyState extends AbstractState {
	private static final Logger LOG = LoggerFactory.getLogger(StandbyState.class);

	public StandbyState(RecoveryManager context) {
		super(context);
	}

	@Override
	public void executeEnter() {

	}

	@Override
	public void notifyStartRecovery() {
		LOG.info("Received start recovery notification!");

		State newState = new WaitingConnectionsState(context);
		context.setState(newState);
	}


	@Override
	public String toString() {
		return "StandbyState{}";
	}
}
