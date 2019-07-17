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
 * Signals that the iteration is completely executed, participating tasks must terminate now.
 */
public class InFlightLogRequestEvent extends TaskEvent {

	private int channelIndex;

	/**
	 * Default constructor (should only be used for deserialization).
	 */
	public InFlightLogRequestEvent() {
		// default constructor implementation.
		// should only be used for deserialization
	}

	public InFlightLogRequestEvent(int channelIndex) {
		this.channelIndex = channelIndex;
	}

	public int getChannelIndex() {
		return channelIndex;
	}


	@Override
	public void write(final DataOutputView out) throws IOException {
		out.writeInt(this.channelIndex);
	}

	@Override
	public void read(final DataInputView in) throws IOException {
		this.channelIndex = in.readInt();
	}

	@Override
	public int hashCode() {

		return this.channelIndex;
	}

	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof InFlightLogRequestEvent)) {
			return false;
		}

		final InFlightLogRequestEvent inFlightLogRequestEvent = (InFlightLogRequestEvent) obj;

		return (this.channelIndex == inFlightLogRequestEvent.getChannelIndex());
	}
}
