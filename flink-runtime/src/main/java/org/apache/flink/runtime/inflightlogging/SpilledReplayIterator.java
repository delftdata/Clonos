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

package org.apache.flink.runtime.inflightlogging;

import org.apache.flink.runtime.io.disk.iomanager.BufferFileReader;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.RequestDoneCallback;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * {@link SpilledReplayIterator} is to be used in combination with {@link SpillableSubpartitionInFlightLogger}.
 * The {@link SpillableSubpartitionInFlightLogger} spills the in-flight log to disk asynchronously, while this
 * {@link InFlightLogIterator} implementation is able to then read those files and regenerate those buffers.
 * This is done deterministically and buffers have the exact same size.
 * <p>
 * To achieve this behaviour we split the Iterator into a producer and a consumer. The producer will first lock
 * the <code>subpartitionLock</code>, preventing any in-memory buffers to be spilled. Then it uses all buffers
 * available in
 * the partition's {@link BufferPool} to create asynchronous read requests to the spill files. It produces these
 * segments through callbacks into separate {@link LinkedBlockingDeque}'s, since each epoch is in a different file,
 * and each file may be served by a separate async IO thread. Not doing so could cause interleavings of messages.
 * <p>
 * The consumer is simple in comparison. It simply checks if the buffer is available in memory, and if it is,
 * returns it. Otherwise, it will check the appropriate deque for the buffer, blocking if necessary.
 */
public class SpilledReplayIterator extends InFlightLogIterator<Buffer> {
	private static final Logger LOG = LoggerFactory.getLogger(SpilledReplayIterator.class);

	private final Object spillLock;
	private final BufferPool recoveryBufferPool;
	private final SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> logToReplay;
	private final IOManager ioManager;


	//The queues to contain buffers	which are asynchronously read
	private ConcurrentMap<Long, LinkedBlockingDeque<Buffer>> readyBuffersPerEpoch;

	//The cursor indicating the consumers position in the log
	private EpochCursor consumerCursor;

	private EpochCursor prefetchCursor;

	//Used to signal back to flusher thread that we are done replaying and that it may resume flushing
	private final AtomicBoolean isReplaying;

	private Map<Long, BufferFileReader> epochReaders;

	public SpilledReplayIterator(SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> logToReplay,
								 BufferPool recoveryBufferPool,
								 IOManager ioManager, Object spillLock, int ignoreBuffers,
								 AtomicBoolean isReplaying) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("SpilledReplayIterator created");
			LOG.debug("State of in-flight log: { {} }",
				logToReplay.entrySet().stream().map(e -> e.getKey() + "->" + e.getValue()).collect(Collectors.joining(
					",")));
		}
		this.consumerCursor = new EpochCursor(logToReplay);
		this.prefetchCursor = new EpochCursor(logToReplay);
		this.isReplaying = isReplaying;
		this.recoveryBufferPool = recoveryBufferPool;

		//skip ignoreBuffers buffers
		for (int i = 0; i < ignoreBuffers; i++) {
			consumerCursor.next();
			prefetchCursor.next();
		}
		this.spillLock = spillLock;
		this.ioManager = ioManager;
		this.logToReplay = logToReplay;

		readyBuffersPerEpoch = new ConcurrentHashMap<>(logToReplay.keySet().size());
		//Initialize the queues
		for (Map.Entry<Long, SpillableSubpartitionInFlightLogger.Epoch> entry : logToReplay.entrySet()) {
			LinkedBlockingDeque<Buffer> queue = new LinkedBlockingDeque<>();
			readyBuffersPerEpoch.put(entry.getKey(), queue);
		}

		this.epochReaders = new HashMap<>(logToReplay.keySet().size());
		for (Map.Entry<Long, SpillableSubpartitionInFlightLogger.Epoch> entry : logToReplay.entrySet()) {
			try {
				epochReaders.put(entry.getKey(), ioManager.createBufferFileReader(entry.getValue().getFileHandle()
					, new ReadCompletedCallback(readyBuffersPerEpoch.get(entry.getKey()))));
			} catch (Exception e) {
				logAndThrowAsRuntimeException(e);
			}
		}

		prefetchNextBuffers();
	}

	private void prefetchNextBuffers() {
		try {
			synchronized (spillLock) {
				while (prefetchCursor.hasNext()) {
					long currentEpoch = prefetchCursor.getNextEpoch();
					Buffer nextBuffer = prefetchCursor.next();
					if (nextBuffer.isRecycled()) {
						BufferFileReader reader = epochReaders.get(currentEpoch);
						//We need to read it from disk into readyBuffers
						Buffer bufferToReadInto = recoveryBufferPool.requestBuffer();
						if (bufferToReadInto == null) {
							prefetchCursor.previous();
							break;
						}
						reader.readInto(bufferToReadInto);
					} else {
						//boolean exchanged = InFlightLoggingUtil.exchangeOwnership(nextBuffer, recoveryBufferPool, null, false);
						//if(!exchanged){
						//	prefetchCursor.previous();
						//	break;
						//}
						//Retain once, so that if a spill completes, it is still in memory.
						nextBuffer.retainBuffer();
					}

				}
				LOG.debug("Prefetched up to offset {} of epoch {}, {} remaining", prefetchCursor.getNextEpochOffset(), prefetchCursor.getNextEpoch(), prefetchCursor.getRemaining());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public int numberRemaining() {
		return consumerCursor.getRemaining();
	}

	@Override
	public long getEpoch() {
		return consumerCursor.getNextEpoch();
	}

	@Override
	public Buffer next() {
		Buffer buffer = null;
		synchronized (spillLock) {
			try {
				while (!consumerCursor.behind(prefetchCursor)) {
					prefetchNextBuffers();
					if(!consumerCursor.behind(prefetchCursor))
						spillLock.wait(5);
				}

				long currentEpoch = consumerCursor.getNextEpoch();
				buffer = consumerCursor.next();
				if (buffer.isRecycled())
					buffer = readyBuffersPerEpoch.get(currentEpoch).take();

				if (!consumerCursor.hasNext()) {
					isReplaying.set(false);
					LOG.info("Done replaying. Closing readers.");
					//close will wait for all requests to complete before closing
					for (BufferFileReader r : epochReaders.values())
						if (!r.isClosed())
							r.close();
				}

				prefetchNextBuffers();
			} catch (InterruptedException | IOException e) {
				Thread.currentThread().interrupt();
				this.close();
			}
		}

		return buffer;
	}

	@Override
	public Buffer peekNext() {
		Buffer buffer = null;
		try {
			synchronized (spillLock) {
				while (!consumerCursor.behind(prefetchCursor)) {
					prefetchNextBuffers();
					if(!consumerCursor.behind(prefetchCursor))
						spillLock.wait(5);
				}

				long currentEpoch = consumerCursor.getNextEpoch();
				buffer = consumerCursor.next();
				if (buffer.isRecycled()) {//Producer will increase refCnt if flush not complete when processed
					buffer = readyBuffersPerEpoch.get(currentEpoch).take();
					//After peeking push it back
					readyBuffersPerEpoch.get(currentEpoch).putFirst(buffer);
				}
				consumerCursor.previous();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			this.close(); //cleanup
		}
		return buffer;
	}

	@Override
	public void close() {
		//Note, there may be a better way to do this if a new iterator is going to be built. We could avoid recycling
		//buffers we will need
		try {
			synchronized (spillLock) {
				while (consumerCursor.hasNext()) {
					while (!consumerCursor.behind(prefetchCursor))
						spillLock.wait(50);

					long currentEpoch = consumerCursor.getNextEpoch();
					Buffer buffer = consumerCursor.next();
					if (buffer.isRecycled())
						buffer = readyBuffersPerEpoch.get(currentEpoch).take();
					buffer.recycleBuffer();
				}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			this.close();
		}
	}

	@Override
	public boolean hasNext() {
		synchronized (spillLock) {
			return consumerCursor.hasNext();
		}
	}

	public void notifyNewBufferAdded(long epochID) {
		synchronized (spillLock) {
			try {
				prefetchCursor.notifyNewBuffer(epochID);
				consumerCursor.notifyNewBuffer(epochID);

				if (!readyBuffersPerEpoch.containsKey(epochID)) {
					readyBuffersPerEpoch.put(epochID, new LinkedBlockingDeque<>());
					epochReaders.put(epochID, ioManager.createBufferFileReader(logToReplay.get(epochID).getFileHandle()
						, new ReadCompletedCallback(readyBuffersPerEpoch.get(epochID))));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}


	private static class ReadCompletedCallback implements RequestDoneCallback<Buffer> {

		private final LinkedBlockingDeque<Buffer> readyDataBuffers;

		public ReadCompletedCallback(LinkedBlockingDeque<Buffer> readyDataBuffers) {
			this.readyDataBuffers = readyDataBuffers;
		}

		@Override
		public void requestSuccessful(Buffer request) {
			try {
				readyDataBuffers.put(request);

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void requestFailed(Buffer buffer, IOException e) {
			String msg = "Read of buffer failed during replay with error: " + e.getMessage();
			LOG.info("Error: " + msg);
			logAndThrowAsRuntimeException(e);
		}
	}

	public static class EpochCursor {

		private final SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> log;
		private long nextEpoch;
		private int nextEpochOffset; //The next buffer the reader will request

		private long lastEpoch;
		private int remaining;

		public EpochCursor(SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> log) {
			this.nextEpoch = log.firstKey();
			this.nextEpochOffset = 0;
			this.lastEpoch = log.lastKey();
			this.remaining =
				log.values().stream().mapToInt(SpillableSubpartitionInFlightLogger.Epoch::getEpochSize).sum();
			this.log = log;
		}

		public boolean hasNext() {
			return remaining > 0;
		}

		public long getNextEpoch() {
			advanceEpochIfNeeded();
			return nextEpoch;
		}

		public int getNextEpochOffset() {
			advanceEpochIfNeeded();
			return nextEpochOffset;
		}

		public int getRemaining() {
			return remaining;
		}

		public Buffer next() {
			advanceEpochIfNeeded();
			SpillableSubpartitionInFlightLogger.Epoch epoch = log.get(nextEpoch);
			List<Buffer> buffers = epoch.getEpochBuffers();
			Buffer toReturn = buffers.get(nextEpochOffset);
			nextEpochOffset++;
			remaining--;
			advanceEpochIfNeeded();
			return toReturn;
		}

		private void advanceEpochIfNeeded() {
			if (reachedEndOfEpoch(nextEpochOffset, nextEpoch))
				if (nextEpoch != lastEpoch) {
					nextEpoch++;
					nextEpochOffset = 0;
				}
		}

		private boolean reachedEndOfEpoch(int offset, long epochID) {
			return offset == log.get(epochID).getEpochSize();
		}

		public boolean behind(EpochCursor other) {
			return this.getNextEpoch() < other.getNextEpoch() ||
				(this.getNextEpoch() == other.getNextEpoch() && this.getNextEpochOffset() < other.getNextEpochOffset());
		}

		public void previous(){
			advanceEpochIfNeeded();
			nextEpochOffset--;
			remaining++;

			if(nextEpochOffset == -1) {
					nextEpoch--;
					nextEpochOffset = log.get(nextEpoch).getEpochSize() - 1;
			}
		}

		@Override
		public String toString() {
			return "EpochCursor{" +
				"currentEpoch=" + nextEpoch +
				", epochOffset=" + nextEpochOffset +
				", remaining=" + remaining +
				'}';
		}

		public void notifyNewBuffer(long epochID) {
			remaining++;
			if (epochID > lastEpoch)
				lastEpoch = epochID;
		}
	}

	private static void logAndThrowAsRuntimeException(Exception e) {
		LOG.error("Error in SpilledReplayIterator", e);
		throw new RuntimeException(e);
	}

}
