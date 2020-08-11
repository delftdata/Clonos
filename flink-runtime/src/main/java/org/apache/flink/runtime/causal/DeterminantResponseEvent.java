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
import org.apache.flink.runtime.causal.determinant.Determinant;
import org.apache.flink.runtime.causal.log.vertex.VertexCausalLog;
import org.apache.flink.runtime.causal.log.vertex.VertexCausalLogDelta;
import org.apache.flink.runtime.event.TaskEvent;

import java.io.IOException;
import java.util.Arrays;

public class DeterminantResponseEvent extends TaskEvent {

	VertexCausalLogDelta vertexCausalLogDelta;
	boolean found;

	public DeterminantResponseEvent() {
	}

	public DeterminantResponseEvent(VertexCausalLogDelta vertexCausalLogDelta) {
		this.found = true;
		this.vertexCausalLogDelta = vertexCausalLogDelta;
	}

	/**
	 * This constructor differentiates from the empty constructor used in deserialization.
	 * Though it receives a parameter found, it is expected that this is always false.
	 */
	public DeterminantResponseEvent(boolean found, VertexID vertexID){
		this.found = found;
		this.vertexCausalLogDelta = new VertexCausalLogDelta(vertexID);
	}


	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeBoolean(found);
		vertexCausalLogDelta.write(out);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.found = in.readBoolean();
		this.vertexCausalLogDelta = new VertexCausalLogDelta();
		this.vertexCausalLogDelta.read(in);

	}

	public VertexCausalLogDelta getVertexCausalLogDelta() {
		return vertexCausalLogDelta;
	}

	public boolean getFound() {
		return found;
	}

	@Override
	public String toString() {
		return "DeterminantResponseEvent{" +
			"found=" + found +
			"vertexCausalLogDelta=" + vertexCausalLogDelta +
			'}';
	}

}
