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
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

public class NetworkBufferBasedContiguousUpstreamThreadCausalLog extends NetworkBufferBasedContiguousThreadCausalLog implements UpstreamThreadCausalLog {

	public NetworkBufferBasedContiguousUpstreamThreadCausalLog(BufferPool bufferPool) {
		super(bufferPool);
	}

	//Multiple producers may call this concurrently, thus we synchronize on buf
	@Override
	public void processUpstreamVertexCausalLogDelta(ThreadLogDelta causalLogDelta, long epochID) {


		int determinantSize = causalLogDelta.getBufferSize();
		if (determinantSize > 0) {
			int offsetFromEpoch = causalLogDelta.getOffsetFromEpoch();

			readLock.lock();
			synchronized (buf) {
				int writeIndex = writerIndex.get();
				EpochStartOffset epochStartOffset = epochStartOffsets.computeIfAbsent(epochID, k -> new EpochStartOffset(k, writeIndex));

				int currentOffsetFromEpoch = writeIndex - epochStartOffset.getOffset();

				int numNewDeterminants = (offsetFromEpoch + determinantSize) - currentOffsetFromEpoch;

				while (notEnoughSpaceFor(numNewDeterminants))
					addComponent();

				ByteBuf deltaBuf = causalLogDelta.getRawDeterminants();
				LOG.info("Processing upstream delta for epoch {}, buf {}, offset {}, currentOffset {}, numNewDeterminants {}, ", epochID, deltaBuf, offsetFromEpoch, currentOffsetFromEpoch, numNewDeterminants);
				deltaBuf.readerIndex(determinantSize - numNewDeterminants);
				//add the new determinants
				buf.writeBytes(deltaBuf, numNewDeterminants);
				writerIndex.addAndGet(numNewDeterminants);
			}

			readLock.unlock();
		}
	}
}
