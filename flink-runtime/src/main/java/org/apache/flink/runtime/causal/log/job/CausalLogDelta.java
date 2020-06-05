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

package org.apache.flink.runtime.causal.log.job;

import org.apache.flink.runtime.causal.log.NettyMessageWritable;
import org.apache.flink.runtime.causal.log.vertex.VertexCausalLogDelta;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;

import java.util.Arrays;

public class CausalLogDelta implements NettyMessageWritable {

	long epochID;
	VertexCausalLogDelta[] vertexCausalLogDeltas;

	public CausalLogDelta(){};

	public CausalLogDelta(long epochID, VertexCausalLogDelta[] vertexCausalLogDeltas){
		this.epochID = epochID;
		this.vertexCausalLogDeltas = vertexCausalLogDeltas;
	}


	public long getEpochID() {
		return epochID;
	}

	public VertexCausalLogDelta[] getVertexCausalLogDeltas() {
		return vertexCausalLogDeltas;
	}

	public void release(){
		for(VertexCausalLogDelta v: vertexCausalLogDeltas)
			v.release();
	}

	@Override
	public int getHeaderSize() {
		return Long.BYTES +  + Short.BYTES + Arrays.stream(vertexCausalLogDeltas).mapToInt(VertexCausalLogDelta::getHeaderSize).sum();
	}

	@Override
	public int getBodySize() {
		return Arrays.stream(vertexCausalLogDeltas).mapToInt(VertexCausalLogDelta::getBodySize).sum();
	}


	@Override
	public void writeHeaderTo(ByteBuf byteBuf) {
		byteBuf.writeLong(epochID);
		byteBuf.writeShort(vertexCausalLogDeltas.length);
		for(VertexCausalLogDelta v : vertexCausalLogDeltas)
			v.writeHeaderTo(byteBuf);
	}

	@Override
	public void writeBodyTo(CompositeByteBuf byteBuf) {
		for(VertexCausalLogDelta v : vertexCausalLogDeltas)
			v.writeBodyTo(byteBuf);
	}

	@Override
	public void readHeaderFrom(ByteBuf byteBuf) {
		this.epochID = byteBuf.readLong();
		short numVertexCausalLogDeltas = byteBuf.readShort();
		vertexCausalLogDeltas = new VertexCausalLogDelta[numVertexCausalLogDeltas];
		for(int i = 0; i < vertexCausalLogDeltas.length; i++) {
			VertexCausalLogDelta newDelta = new VertexCausalLogDelta();
			newDelta.readHeaderFrom(byteBuf);
			vertexCausalLogDeltas[i] = newDelta;
		}
	}

	@Override
	public void readBodyFrom(ByteBuf byteBuf) {
		for(int i = 0; i < vertexCausalLogDeltas.length; i++){
			vertexCausalLogDeltas[i].readBodyFrom(byteBuf);
		}
	}

}
