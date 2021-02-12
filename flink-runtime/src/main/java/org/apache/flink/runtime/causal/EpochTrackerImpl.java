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

package org.apache.flink.runtime.causal;


import org.apache.flink.runtime.causal.recovery.IRecoveryManager;
import org.apache.flink.runtime.state.CheckpointListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * All interaction with the record counter should be done inside the checkpoint lock.
 */
public final class EpochTrackerImpl implements EpochTracker {

	private static final Logger LOG = LoggerFactory.getLogger(EpochTrackerImpl.class);

	public static final int NO_RECORD_COUNT_TARGET = -1;

	//The current input record count
	private int recordCount;

	//The input record count target at which the recovery manager should be notified.
	private int recordCountTarget;

	private IRecoveryManager recoveryManager;

	private long currentEpoch;

	private final List<EpochStartListener> epochStartListeners;
	private final List<CheckpointListener> checkpointCompleteListeners;

	public EpochTrackerImpl() {
		currentEpoch = 0;
		recordCount = 0;
		recordCountTarget = NO_RECORD_COUNT_TARGET;
		epochStartListeners = new ArrayList<>(10);
		checkpointCompleteListeners = new ArrayList<>(10);
	}


	@Override
	public void setRecoveryManager(IRecoveryManager recoveryManager) {
		this.recoveryManager = recoveryManager;
	}

	@Override
	public long getCurrentEpoch() {
		return currentEpoch;
	}

	@Override
	public final int getRecordCount() {
		return recordCount;
	}

	@Override
	public final void incRecordCount() {
		recordCount++;

		if (LOG.isDebugEnabled())
			LOG.debug("incRecordCount: current={}, target={}", recordCount, recordCountTarget);
	}

	@Override
	public final void startNewEpoch(long epochID) {
		recordCount = 0;
		currentEpoch = epochID;
		if (LOG.isDebugEnabled())
			LOG.debug("resetRecordCount: current={}, target={}", recordCount, recordCountTarget);
		for (EpochStartListener listener : epochStartListeners)
			listener.notifyEpochStart(epochID);
	}

	@Override
	public void setRecordCountTarget(int target) {
		this.recordCountTarget = target;
		if (LOG.isDebugEnabled())
			LOG.debug("setRecordCountTarget: current={}, target={}", recordCount, recordCountTarget);
	}



	@Override
	public void subscribeToEpochStartEvents(EpochStartListener listener) {
		this.epochStartListeners.add(listener);
	}

	@Override
	public void subscribeToCheckpointCompleteEvents(CheckpointListener listener) {
		this.checkpointCompleteListeners.add(listener);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		LOG.info("Notified of checkpoint complete {}", checkpointId);
		for (CheckpointListener c : checkpointCompleteListeners) {
			try {
				c.notifyCheckpointComplete(checkpointId);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
