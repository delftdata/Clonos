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

package org.apache.flink.runtime.causal.determinant;

import org.apache.flink.runtime.causal.recovery.RecoveryManager;

public class IgnoreCheckpointDeterminant extends AsyncDeterminant{

	private long checkpointID;


	public IgnoreCheckpointDeterminant(){

	}

	public IgnoreCheckpointDeterminant(int recordCount, long checkpointID){
		super(recordCount);
		this.checkpointID = checkpointID;

	}
	public IgnoreCheckpointDeterminant replace(int recordCount, long checkpointID){
		this.recordCount = recordCount;
		this.checkpointID = checkpointID;
		return this;
	}

	public long getCheckpointID() {
		return checkpointID;
	}

	@Override
	public void process(RecoveryManager recoveryManager) {
		recoveryManager.getCheckpointForceable().ignoreCheckpoint(this.checkpointID);
	}

	@Override
	public String toString() {
		return "IgnoreCheckpointDeterminant{" +
			"recordCount=" + recordCount +
			", checkpointID=" + checkpointID +
			'}';
	}


	@Override
	public int getEncodedSizeInBytes() {
		return super.getEncodedSizeInBytes() + Long.BYTES;
	}

	public static byte getTypeTag() {
		return IGNORE_CHECKPOINT_DETERMINANT;
	}
}
