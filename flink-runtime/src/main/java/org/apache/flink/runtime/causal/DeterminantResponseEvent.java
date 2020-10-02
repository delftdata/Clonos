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

import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.causal.log.job.CausalLogID;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class DeterminantResponseEvent extends TaskEvent {

	private boolean found;

	private VertexID vertexID;

	private Map<CausalLogID, ByteBuf> determinants;

	public DeterminantResponseEvent() {
	}

	public DeterminantResponseEvent(VertexID failedVertex) {
		this(false, failedVertex);
	}

	/**
	 * This constructor differentiates from the empty constructor used in deserialization.
	 * Though it receives a parameter found, it is expected that this is always false.
	 */
	public DeterminantResponseEvent(boolean found, VertexID vertexID) {
		this.found = found;
		this.vertexID = vertexID;
		determinants = new HashMap<>();
	}

	public DeterminantResponseEvent(VertexID vertexID, Map<CausalLogID, ByteBuf> determinants) {
		this.found = true;
		this.vertexID = vertexID;
		this.determinants = determinants;
	}


	public boolean isFound() {
		return found;
	}

	public VertexID getVertexID() {
		return vertexID;
	}

	public Map<CausalLogID, ByteBuf> getDeterminants() {
		return determinants;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeBoolean(found);
		out.writeShort(vertexID.getVertexId());
		out.writeByte(determinants.size());
		for (Map.Entry<CausalLogID, ByteBuf> entry : determinants.entrySet()) {
			entry.getKey().write(out);
			ByteBuf buf = entry.getValue();
			out.writeInt(buf.readableBytes());
			buf.readBytes(new DataOutputViewStream(out), buf.readableBytes());
			buf.release();
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.found = in.readBoolean();
		this.vertexID = new VertexID(in.readShort());
		this.determinants = new HashMap<>();
		byte numDeterminantDeltas = in.readByte();
		for (int i = 0; i < numDeterminantDeltas; i++) {
			CausalLogID causalLogID = new CausalLogID();
			causalLogID.read(in);

			int numBytesOfBuf = in.readInt();
			byte[] toWrap = new byte[numBytesOfBuf];
			in.read(toWrap);
			ByteBuf buf = Unpooled.wrappedBuffer(toWrap);

			this.determinants.put(causalLogID, buf);
		}
	}


	public void merge(DeterminantResponseEvent other){

		if(!this.found && !other.found)
			return;

		if(!this.found) //The other one is found
			this.found = true;

		for(Map.Entry<CausalLogID, ByteBuf> entry : other.determinants.entrySet()) {
			determinants.merge(entry.getKey(), entry.getValue(), (v1, v2) -> {
				//Note, this is only ran if both are defined, in which  case the smaller one is released.
				if(v1.readableBytes() > v2.readableBytes()) {
					v2.release();
					return v1;
				}else {
					v1.release();
					return v2;
				}
			} );
		}
	}

	@Override
	public String toString() {
		return "DeterminantResponseEvent{" +
			"found=" + found +
			", vertexID=" + vertexID +
			", determinants=[" + determinants.entrySet().stream().map(e-> e.getKey() + " -> " + e.getValue().readableBytes()).collect(Collectors.joining(", ")) +
			"]}";
	}
}
