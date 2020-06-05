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

import org.apache.flink.runtime.io.network.buffer.BufferPool;

public class NetworkBufferBasedContiguousLocalThreadCausalLog extends NetworkBufferBasedContiguousThreadCausalLog implements LocalThreadCausalLog {

	public NetworkBufferBasedContiguousLocalThreadCausalLog(BufferPool bufferPool) {
		super(bufferPool);
	}

	//This method has a single producer, thus we do not need to synchronize accesses.
	@Override
	public void appendDeterminants(byte[] determinants, long epochID) {
		readLock.lock();
		int writeIndex = writerIndex.get();
		epochStartOffsets.computeIfAbsent(epochID, k -> new EpochStartOffset(k, writeIndex));

		while (notEnoughSpaceFor(determinants.length))
			addComponent();

		buf.writeBytes(determinants);
		writerIndex.addAndGet(determinants.length);
		readLock.unlock();
	}
}
