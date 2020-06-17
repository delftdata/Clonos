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

package org.apache.flink.runtime.causal.log.thread;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

public class ThreadLogDelta {

	/**
	 * The determinants themselves
	 */
	ByteBuf rawDeterminants;

	/**
	 * The offset from the current epoch.
	 */
	int offsetFromEpoch;

	int bufferSize;

	//main thread constructor
	public ThreadLogDelta(ByteBuf rawDeterminants, int offsetFromEpoch) {
		this.rawDeterminants = rawDeterminants;
		this.offsetFromEpoch = offsetFromEpoch;
		this.bufferSize = rawDeterminants.readableBytes();
	}

	public ThreadLogDelta(int offsetFromEpoch, int bufferSize){
		this.offsetFromEpoch = offsetFromEpoch;
		this.bufferSize = bufferSize;
	}

	public ByteBuf getRawDeterminants() {
		return rawDeterminants;
	}

	public void setRawDeterminants(ByteBuf rawDeterminants){
		this.rawDeterminants = rawDeterminants;
		this.bufferSize = rawDeterminants.readableBytes();
	}

	public int getOffsetFromEpoch() {
		return offsetFromEpoch;
	}

	public int getDeltaSize() {
		return bufferSize;
	}

	public void merge(ThreadLogDelta delta) {


	}

	@Override
	public String toString() {
		return "ThreadLogDelta{" +
			"rawDeterminants=" + rawDeterminants +
			", offsetFromEpoch=" + offsetFromEpoch +
			", bufferSize=" + bufferSize +
			'}';
	}
}
