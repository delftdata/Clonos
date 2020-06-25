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

import org.apache.flink.runtime.causal.determinant.Determinant;
import org.apache.flink.runtime.causal.determinant.DeterminantEncoder;
import org.apache.flink.runtime.io.network.buffer.BufferPool;

public final class NetworkBufferBasedContiguousLocalThreadCausalLog extends NetworkBufferBasedContiguousThreadCausalLog implements LocalThreadCausalLog {

	DeterminantEncoder encoder;

	public NetworkBufferBasedContiguousLocalThreadCausalLog(BufferPool bufferPool, DeterminantEncoder encoder) {
		super(bufferPool);
		this.encoder = encoder;
	}

	// A local log has a single producer, thus we do not need to synchronize accesses.
	// writerIndex is used to control visibility for consumers, by only making available bytes that are fully written.
	@Override
	public void appendDeterminant(Determinant determinant, long epochID) {
		int determinantEncodedSize = determinant.getEncodedSizeInBytes();
		readLock.lock();
		try {
			int writeIndex = writerIndex.get();
			epochStartOffsets.computeIfAbsent(epochID, k -> new EpochStartOffset(k, writeIndex));
			while (notEnoughSpaceFor(determinantEncodedSize))
				addComponent();

			encoder.encodeTo(determinant, buf);
			writerIndex.addAndGet(determinantEncodedSize);
		} finally {
			readLock.unlock();
		}
	}
}
