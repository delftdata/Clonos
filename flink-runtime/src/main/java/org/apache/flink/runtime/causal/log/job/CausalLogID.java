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


import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * An id identifying a thread causal log.
 */
public class CausalLogID implements IOReadableWritable {

	private short vertexID;
	private boolean isMainThread;
	private long intermediateDataSetUpper;
	private long intermediateDataSetLower;
	private byte subpartitionIndex;

	public CausalLogID(CausalLogID other) {
		this.vertexID = other.vertexID;
		this.isMainThread = other.isMainThread;
		this.intermediateDataSetLower = other.intermediateDataSetLower;
		this.intermediateDataSetUpper = other.intermediateDataSetUpper;
		this.subpartitionIndex = other.subpartitionIndex;
	}

	public CausalLogID(short vertexID) {
		this.vertexID = vertexID;
		isMainThread = true;
	}

	public CausalLogID(short vertexID, long intermediateDataSetLower, long intermediateDataSetUpper,
					   byte subpartitionIndex) {
		this.vertexID = vertexID;
		this.isMainThread = false;
		this.intermediateDataSetLower = intermediateDataSetLower;
		this.intermediateDataSetUpper = intermediateDataSetUpper;
		this.subpartitionIndex = subpartitionIndex;
	}

	public CausalLogID() {

	}

	public short getVertexID() {
		return vertexID;
	}

	public long getIntermediateDataSetUpper() {
		return intermediateDataSetUpper;
	}

	public long getIntermediateDataSetLower() {
		return intermediateDataSetLower;
	}

	public byte getSubpartitionIndex() {
		return subpartitionIndex;
	}

	public boolean isForVertex(short vertexID) {
		return this.vertexID == vertexID;
	}

	public boolean isMainThread() {
		return isMainThread;
	}

	public boolean isForIntermediatePartition(long lower, long upper) {
		return this.intermediateDataSetLower == lower && this.intermediateDataSetUpper == upper;
	}

	public boolean isForSubpartition(byte index) {
		return this.subpartitionIndex == index;
	}

	public CausalLogID replace(short vertexID) {
		this.vertexID = vertexID;
		isMainThread = true;
		return this;
	}

	public CausalLogID replace(short vertexID, long intermediateDataSetLower, long intermediateDataSetUpper,
							   byte subpartitionIndex) {
		this.vertexID = vertexID;
		this.isMainThread = false;
		this.intermediateDataSetLower = intermediateDataSetLower;
		this.intermediateDataSetUpper = intermediateDataSetUpper;
		this.subpartitionIndex = subpartitionIndex;
		return this;
	}

	public CausalLogID replace(long intermediateDataSetLower, long intermediateDataSetUpper, byte subpartitionIndex) {
		this.isMainThread = false;
		this.intermediateDataSetLower = intermediateDataSetLower;
		this.intermediateDataSetUpper = intermediateDataSetUpper;
		this.subpartitionIndex = subpartitionIndex;
		return this;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		CausalLogID that = (CausalLogID) o;

		if(this.vertexID != that.vertexID)
			return false;

		if (this.isMainThread != that.isMainThread) //Different types => false
			return false;

		//At this point, we know that matching vertexIDs and matching is MainThread.
		//If one isMainThread, the other also is, and they match.
		if(this.isMainThread)
			return true;

		//Otherwise, both subpartition => compare subpartitionIDs
		return intermediateDataSetUpper == that.intermediateDataSetUpper &&
			intermediateDataSetLower == that.intermediateDataSetLower &&
			subpartitionIndex == that.subpartitionIndex;
	}

	@Override
	public int hashCode() {
		int hash = 17;
		hash = 31 * hash + vertexID;
		hash = 31 * hash + (isMainThread ? 1 : 0);
		if (isMainThread)
			return hash;

		hash = 31 * hash + (int) (intermediateDataSetLower ^ (intermediateDataSetLower >>> 32));
		hash = 31 * hash + (int) (intermediateDataSetUpper ^ (intermediateDataSetUpper >>> 32));
		hash = 31 * hash + subpartitionIndex;
		return hash;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeShort(vertexID);
		out.writeBoolean(isMainThread);
		if (isMainThread)
			return;
		out.writeLong(intermediateDataSetLower);
		out.writeLong(intermediateDataSetUpper);
		out.writeByte(subpartitionIndex);

	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.vertexID = in.readShort();
		this.isMainThread = in.readBoolean();
		if (isMainThread)
			return;
		this.intermediateDataSetLower = in.readLong();
		this.intermediateDataSetUpper = in.readLong();
		this.subpartitionIndex = in.readByte();
	}

	@Override
	public String toString() {
		return "CausalLogID{" +
			"vertexID=" + vertexID +
			", isMainThread=" + isMainThread + (isMainThread ? "}" :
			", intermediateDataSetUpper=" + intermediateDataSetUpper +
				", intermediateDataSetLower=" + intermediateDataSetLower +
				", subpartitionIndex=" + subpartitionIndex +
				'}');
	}
}
