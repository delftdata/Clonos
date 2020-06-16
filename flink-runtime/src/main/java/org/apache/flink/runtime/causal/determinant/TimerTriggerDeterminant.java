/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.causal.determinant;

import org.apache.flink.runtime.causal.recovery.RecoveryManager;

public class TimerTriggerDeterminant extends NonMainThreadDeterminant {

	ProcessingTimeCallbackID processingTimeCallbackID;
	private int recordCount;
	private long timestamp;

	public TimerTriggerDeterminant(ProcessingTimeCallbackID processingTimeCallbackID, int recordCount, long timestamp){
		this.processingTimeCallbackID = processingTimeCallbackID;
		this.recordCount = recordCount;
		this.timestamp = timestamp;
	}

	public ProcessingTimeCallbackID getProcessingTimeCallbackID() {
		return processingTimeCallbackID;
	}

	public int getRecordCount() {
		return recordCount;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public void process(RecoveryManager recoveryManager) {
		recoveryManager.processingTimeForceable.forceExecution(processingTimeCallbackID, timestamp);
	}

	@Override
	public String toString() {
		return "TimerTriggerDeterminant{" +
			"processingTimeCallbackID=" + processingTimeCallbackID +
			", recordCount=" + recordCount +
			", timestamp=" + timestamp +
			'}';
	}
}
