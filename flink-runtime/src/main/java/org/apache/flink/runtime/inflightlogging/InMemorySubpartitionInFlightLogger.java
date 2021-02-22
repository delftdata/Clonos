/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional debugrmation
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
package org.apache.flink.runtime.inflightlogging;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class InMemorySubpartitionInFlightLogger implements InFlightLog {

	private static final Logger LOG = LoggerFactory.getLogger(InMemorySubpartitionInFlightLogger.class);

	private final ArrayList<Buffer> log;
	private BufferPool inFlightBufferPool;

	public InMemorySubpartitionInFlightLogger() {
		log = new ArrayList<>();
	}

	@Override
	public void registerBufferPool(BufferPool bufferPool) {
		this.inFlightBufferPool = bufferPool;
	}

	public void log(Buffer buffer, boolean isFinished) {
		synchronized (log) {
			log.add(buffer.retainBuffer());
			if (isFinished)
				InFlightLoggingUtil.exchangeOwnership(buffer, inFlightBufferPool, true);
			LOG.debug("Logged a new buffer");
		}
	}

	@Override
	public void notifyDownstreamCheckpointComplete(int numBuffersProcessedDownstream) {
		synchronized (log) {
			for (int i = 0; i < numBuffersProcessedDownstream; i++)
				log.get(i).recycleBuffer();
			log.subList(0, numBuffersProcessedDownstream).clear();
			LOG.info("InFlightLog: Removed {} buffers.", numBuffersProcessedDownstream);
		}
	}

	@Override
	public InFlightLogIterator<Buffer> getInFlightIterator() {
		synchronized (log) {
			//The lower network stack recycles buffers, so for each replay, we must increase reference counts
			for (Buffer buffer : log)
				buffer.retainBuffer();

			return new ReplayIterator(log);
		}
	}

	@Override
	public void destroyBufferPools() {

	}

	@Override
	public void close() {
		for (Buffer b : log)
			b.recycleBuffer();
	}

	@Override
	public BufferPool getInFlightBufferPool() {
		return inFlightBufferPool;
	}

	public static class ReplayIterator extends InFlightLogIterator<Buffer> {
		private final List<Buffer> log;
		private int currentIndex;

		public ReplayIterator(List<Buffer> log) {
			this.log = log;
			this.currentIndex = 0;
		}

		@Override
		public boolean hasNext() {
			synchronized (log) {
				//if currentIndex == log.size()  then we are done
				return currentIndex < log.size();
			}
		}

		@Override
		public Buffer next() {
			synchronized (log) {
				return log.get(currentIndex++);
			}
		}

		@Override
		public Buffer peekNext() {
			synchronized (log) {
				return log.get(currentIndex);
			}
		}

		@Override
		public void close() {
			synchronized (log) {
				while (this.hasNext())
					this.next().recycleBuffer();
			}
		}

		@Override
		public int numberRemaining() {
			synchronized (log) {
				return log.size() - currentIndex; //if size is 1 and index is 0, there is 1 remaining
			}
		}


	}


}
