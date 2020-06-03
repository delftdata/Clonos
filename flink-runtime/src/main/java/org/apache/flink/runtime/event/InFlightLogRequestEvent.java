/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.event;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.IOException;

/**
 * Event sent from downstream for replaying in-flight tuples for specific output channel, that is subpartition index, starting from the next appointed checkpoint.
 */
public class InFlightLogRequestEvent extends TaskEvent {

	private IntermediateResultPartitionID intermediateResultPartitionID;
	private int subpartitionIndex;
	private long checkpointId;
	private int numberOfBuffersToSkip;

	/**
	 * Default constructor (should only be used for deserialization).
	 */
	public InFlightLogRequestEvent() {
		// default constructor implementation.
		// should only be used for deserialization
	}


	public InFlightLogRequestEvent(IntermediateResultPartitionID intermediateResultPartitionID, int consumedSubpartitionIndex, long finalRestoreStateCheckpointId) {
		this(intermediateResultPartitionID, consumedSubpartitionIndex, finalRestoreStateCheckpointId, 0);
	}

	public InFlightLogRequestEvent(IntermediateResultPartitionID intermediateResultPartitionID, int consumedSubpartitionIndex, long finalRestoreStateCheckpointId, int numberOfBuffersToSkip) {
		super();
		this.intermediateResultPartitionID = intermediateResultPartitionID;
		this.subpartitionIndex = consumedSubpartitionIndex;
		this.checkpointId = finalRestoreStateCheckpointId;
		this.numberOfBuffersToSkip = numberOfBuffersToSkip;
	}

	public IntermediateResultPartitionID getIntermediateResultPartitionID() {
		return intermediateResultPartitionID;
	}

	public int getSubpartitionIndex() {
		return subpartitionIndex;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	public int getNumberOfBuffersToSkip(){
		return numberOfBuffersToSkip;
	}

	@Override
	public void write(final DataOutputView out) throws IOException {
		out.writeLong(intermediateResultPartitionID.getUpperPart());
		out.writeLong(intermediateResultPartitionID.getLowerPart());
		out.writeInt(this.subpartitionIndex);
		out.writeLong(this.checkpointId);
		out.writeInt(numberOfBuffersToSkip);
	}

	@Override
	public void read(final DataInputView in) throws IOException {
		long upper = in.readLong();
		long lower = in.readLong();
		this.intermediateResultPartitionID = new IntermediateResultPartitionID(lower, upper);

		this.subpartitionIndex = in.readInt();
		this.checkpointId = in.readLong();
		this.numberOfBuffersToSkip = in.readInt();
	}

	@Override
	public String toString() {
		return "InFlightLogRequestEvent{" +
			"intermediateResultPartitionID=" + intermediateResultPartitionID +
			", subpartitionIndex=" + subpartitionIndex +
			", checkpointId=" + checkpointId +
			", numberBuffersSkip=" + numberOfBuffersToSkip +
			'}';
	}
}
