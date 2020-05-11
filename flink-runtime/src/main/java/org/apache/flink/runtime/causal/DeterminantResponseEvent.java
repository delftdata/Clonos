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
package org.apache.flink.runtime.causal;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.TaskEvent;

import java.io.IOException;
import java.util.Arrays;

public class DeterminantResponseEvent extends TaskEvent {

	VertexId vertexId;
	byte[] determinants;

	public DeterminantResponseEvent(VertexId vertexId, byte[] determinants) {
		this.vertexId = vertexId;
		this.determinants = determinants;
	}

	public DeterminantResponseEvent() {
	}


	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeShort(vertexId.getVertexId());
		out.writeInt(determinants.length);
		out.write(determinants);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.vertexId = new VertexId(in.readShort());

		int logDeltaLength = in.readInt();
		byte[] logDelta = new byte[logDeltaLength];
		in.read(logDelta);
		this.determinants = logDelta;

	}

	public VertexId getVertexId() {
		return vertexId;
	}

	public byte[] getDeterminants() {
		return determinants;
	}

	@Override
	public String toString() {
		return "DeterminantResponseEvent{" +
			"vertexId=" + vertexId +
			", determinants=" + Arrays.toString(determinants) +
			'}';
	}
}
