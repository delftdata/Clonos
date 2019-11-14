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
import org.apache.flink.runtime.event.TaskEvent;

import java.io.IOException;

/**
 * Event sent from downstream for preparing/replaying in-flight tuples for specific output channel, that is subpartition index, starting from the next appointed checkpoint.
 */
public class InFlightLogEvent extends TaskEvent {

	protected int subpartitionIndex;
	protected long checkpointId;

	/**
	 * Default constructor (should only be used for deserialization).
	 */
	public InFlightLogEvent() {
		// default constructor implementation.
		// should only be used for deserialization
	}

	public InFlightLogEvent(int subpartitionIndex, long checkpointId) {
		this.subpartitionIndex = subpartitionIndex;
		this.checkpointId = checkpointId;
	}

	public int getSubpartitionIndex() {
		return subpartitionIndex;
	}

	public long getCheckpointId() {
		return checkpointId;
	}


	@Override
	public void write(final DataOutputView out) throws IOException {
		out.writeInt(this.subpartitionIndex);
		out.writeLong(this.checkpointId);
	}

	@Override
	public void read(final DataInputView in) throws IOException {
		this.subpartitionIndex = in.readInt();
		this.checkpointId = in.readLong();
	}

	@Override
	public int hashCode() {
		return this.subpartitionIndex;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!(obj instanceof InFlightLogEvent)) {
			return false;
		}

		final InFlightLogEvent inFlightLogEvent = (InFlightLogEvent) obj;
		return (this.subpartitionIndex == inFlightLogEvent.getSubpartitionIndex() && this.checkpointId == inFlightLogEvent.getCheckpointId());
	}

	@Override
	public String toString() {
		return String.format("[subpartitionIndex: %d, checkpointId: %d]", subpartitionIndex, checkpointId);
	}
}
