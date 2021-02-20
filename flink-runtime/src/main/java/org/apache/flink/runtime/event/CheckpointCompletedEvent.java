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
 * Event sent from operator that completed a local checkpoint (SEEP) to the upstream tasks to truncate their in-flight log respectively.
 */
public class CheckpointCompletedEvent extends TaskEvent {

	private IntermediateResultPartitionID intermediateResultPartitionID;
	private int subpartitionIndex;
	private int numberBuffersRemoved;

	/**
	 * Default constructor (should only be used for deserialization).
	 */
	public CheckpointCompletedEvent() {
		// default constructor implementation.
		// should only be used for deserialization
	}

	public CheckpointCompletedEvent(IntermediateResultPartitionID intermediateResultPartitionID, int consumedSubpartitionIndex,
			int numberBuffersRemoved) {
		super();
		this.intermediateResultPartitionID = intermediateResultPartitionID;
		this.subpartitionIndex = consumedSubpartitionIndex;
		this.numberBuffersRemoved = numberBuffersRemoved;
	}

	public IntermediateResultPartitionID getIntermediateResultPartitionID() {
		return intermediateResultPartitionID;
	}

	public int getSubpartitionIndex() {
		return subpartitionIndex;
	}

	public int getNumberBuffersRemoved() {
		return numberBuffersRemoved;
	}

	@Override
	public void write(final DataOutputView out) throws IOException {
		out.writeLong(intermediateResultPartitionID.getUpperPart());
		out.writeLong(intermediateResultPartitionID.getLowerPart());
		out.writeInt(this.subpartitionIndex);
		out.writeInt(this.numberBuffersRemoved);
	}

	@Override
	public void read(final DataInputView in) throws IOException {
		long upper = in.readLong();
		long lower = in.readLong();
		this.intermediateResultPartitionID = new IntermediateResultPartitionID(lower, upper);

		this.subpartitionIndex = in.readInt();
		this.numberBuffersRemoved = in.readInt();
	}

	@Override
	public String toString() {
		return "CheckpointCompletedEvent{" +
			"intermediateResultPartitionID=" + intermediateResultPartitionID +
			", subpartitionIndex=" + subpartitionIndex +
			", numberBuffersRemoved=" + numberBuffersRemoved +
			'}';
	}
}
