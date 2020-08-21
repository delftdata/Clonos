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

import org.apache.flink.runtime.causal.CheckpointForceable;
import org.apache.flink.runtime.causal.DeterminantResponseEvent;
import org.apache.flink.runtime.causal.ProcessingTimeForceable;
import org.apache.flink.runtime.causal.RecordCountTargetForceable;
import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.io.network.api.DeterminantRequestEvent;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.util.List;

public interface IRecoveryManager {


	void notifyNewInputChannel(RemoteInputChannel channel, int consumedSupartitionIndex, int numBuffersRemoved);

	void notifyNewOutputChannel(IntermediateResultPartitionID partitionId, int index);

	void notifyInFlightLogRequestEvent(InFlightLogRequestEvent e);

	void notifyDeterminantResponseEvent(DeterminantResponseEvent e);

	void notifyDeterminantRequestEvent(DeterminantRequestEvent e,int channelRequestArrivedFrom);

	void notifyStateRestorationStart(long checkpointId);

	void notifyStateRestorationComplete(long checkpointId);

	void notifyStartRecovery();

	void triggerAsyncEvent();

	//=======================================

	void setState(State state);

	void setInputGate(InputGate inputGate);

	void setProcessingTimeService(ProcessingTimeForceable processingTimeForceable);

	ProcessingTimeForceable getProcessingTimeForceable();

	CheckpointForceable getCheckpointForceable();

	void setRecordCountTargetForceable(RecordCountTargetForceable recordCountTargetForceable);

	boolean isRunning();

	boolean isReplaying();

	boolean isRestoringState();

	boolean isWaitingConnections();

    boolean isRecovering();

	//====================================================
	/*
	The following methods must be called from deterministic contexts. Otherwise it will
	cause everything to blow up.
	 */

	int replayRandomInt();

	byte replayNextChannel();

	long replayNextTimestamp();

}
