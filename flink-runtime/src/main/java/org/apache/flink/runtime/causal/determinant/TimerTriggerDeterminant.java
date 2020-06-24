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
	private long timestamp;

	public TimerTriggerDeterminant(){
		this.processingTimeCallbackID = new ProcessingTimeCallbackID();
	}

	public TimerTriggerDeterminant(int recordCount, ProcessingTimeCallbackID processingTimeCallbackID, long timestamp){
		super(recordCount);
		this.processingTimeCallbackID = processingTimeCallbackID;
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

	public TimerTriggerDeterminant replace(int recordCount, ProcessingTimeCallbackID processingTimeCallbackID, long timestamp){
		super.replace(recordCount);
		this.processingTimeCallbackID = processingTimeCallbackID;
		this.timestamp = timestamp;
		return this;
	}

	public TimerTriggerDeterminant replace(int recordCount, String callbackIDName, long timestamp){
		super.replace(recordCount);
		this.processingTimeCallbackID.replace(callbackIDName);
		this.timestamp = timestamp;
		return this;
	}

	public TimerTriggerDeterminant replace(int recordCount, ProcessingTimeCallbackID.Type type, long timestamp){
		super.replace(recordCount);
		this.processingTimeCallbackID.replace(type);
		this.timestamp = timestamp;
		return this;
	}

	@Override
	public void process(RecoveryManager recoveryManager) {
		recoveryManager.getProcessingTimeForceable().forceExecution(processingTimeCallbackID, timestamp);
	}

	@Override
	public String toString() {
		return "TimerTriggerDeterminant{" +
			"processingTimeCallbackID=" + processingTimeCallbackID +
			", recordCount=" + recordCount +
			", timestamp=" + timestamp +
			'}';
	}

	@Override
	public int getEncodedSizeInBytes() {
		// size of super, long for ts, 1 for id type ordinal, size of name if exists and name if exists.
		return super.getEncodedSizeInBytes() + Long.BYTES + Byte.BYTES + (processingTimeCallbackID.getType() != ProcessingTimeCallbackID.Type.INTERNAL ?
			0 : Integer.BYTES + processingTimeCallbackID.getName().getBytes().length);
	}
}
