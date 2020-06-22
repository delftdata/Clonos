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
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;

public class SourceCheckpointDeterminant extends NonMainThreadDeterminant{


	private final byte[] storageReference;
	private final long checkpointID;
	private final long checkpointTimestamp;
	private final CheckpointType type;

	public SourceCheckpointDeterminant(int recordCount, long checkpointID, long checkpointTimestamp, CheckpointType type, byte[] storageReference){
		super(recordCount);
		this.checkpointID = checkpointID;
		this.checkpointTimestamp = checkpointTimestamp;
		this.type = type;
		this.storageReference = storageReference;
	}

	@Override
	public String toString() {
		return "SourceCheckpointDeterminant{" +
			"recordCount=" + recordCount +
			'}';
	}

	public byte[] getStorageReference() {
		return storageReference;
	}

	public long getCheckpointID() {
		return checkpointID;
	}

	public long getCheckpointTimestamp() {
		return checkpointTimestamp;
	}

	public CheckpointType getType() {
		return type;
	}

	@Override
	public void process(RecoveryManager recoveryManager) {
		CheckpointMetrics metrics = new CheckpointMetrics()
			.setBytesBufferedInAlignment(0L)
			.setAlignmentDurationNanos(0L);

		try {
			recoveryManager.getCheckpointForceable().performCheckpoint(
				new CheckpointMetaData(checkpointID,checkpointTimestamp),
				new CheckpointOptions(type, (storageReference.length > 0 ? new CheckpointStorageLocationReference(storageReference) : CheckpointStorageLocationReference.getDefault())),
				metrics);
		} catch (Exception e) {
			e.printStackTrace();
		};
	}
}
