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

import org.apache.flink.runtime.io.disk.iomanager.*;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;

/**
 * An inflight logger that periodically flushed available buffers according to a policy.
 * Due to limitations of the asynchronous file api, we must flush each epoch sequentially. Otherwise we may accidentally
 * free a buffer that has not been written yet. This is because each file is output to by a different thread.
 * <p>
 * We start a different file for each epoch so that it may be deleted upon completion.
 */
public class SpillableSubpartitionInFlightLogger implements InFlightLog {

	private static final Logger LOG = LoggerFactory.getLogger(SpillableSubpartitionInFlightLogger.class);

	private final SortedMap<Long, Epoch> slicedLog;
	private final IOManager ioManager;

	private final RequestDoneCallback<Buffer> CALLBACK = new FlushCompletedCallback();

	private final Object flushLock = new Object();

	@GuardedBy("flushLock")
	private long currentAsyncStoreEpoch;

	private final PipelinedSubpartition toLog;

	private final Predicate<SpillableSubpartitionInFlightLogger> flushPolicy;

	public SpillableSubpartitionInFlightLogger(IOManager ioManager, PipelinedSubpartition toLog, Predicate<SpillableSubpartitionInFlightLogger> flushPolicy) {
		this.ioManager = ioManager;
		this.toLog = toLog;
		this.flushPolicy = flushPolicy;

		slicedLog = new TreeMap<>();
		currentAsyncStoreEpoch = -1;
	}

	@Override
	public void log(Buffer buffer, long epochID) {
		Epoch epoch = slicedLog.computeIfAbsent(epochID, k -> new Epoch(createNewWriter()));
		epoch.append(buffer);
		//If we have fully flushed this epoch and there is a next epoch
		synchronized (flushLock) {
			if (currentAsyncStoreEpoch == -1) //initialize if uninitialized
				currentAsyncStoreEpoch = epochID;
			if (flushPolicy.test(this)) {
				epoch.flushAllUnflushed();
			}
		}
		LOG.debug("Logged a new buffer for epoch {}", epochID);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		LOG.debug("Got notified of checkpoint {} completion", checkpointId);
		List<Long> toRemove = new LinkedList<>();

		//keys are in ascending order
		for (long epochId : slicedLog.keySet()) {
			if (epochId < checkpointId) {
				toRemove.add(epochId);
				LOG.debug("Removing epoch {}", epochId);
			}
		}
		synchronized (flushLock) {
			for (long checkpointBarrierId : toRemove)
				slicedLog.remove(checkpointBarrierId).removeEpochFile();
		}

	}

	@Override
	public InFlightLogIterator<Buffer> getInFlightIterator(long epochID) {
		return new SpilledReplayIterator(slicedLog, epochID, this.toLog.getParent().getBufferPool(), ioManager, flushLock);
	}

	public float poolAvailability() {
		BufferPool pool = this.toLog.getParent().getBufferPool();

		return 1 - ((float) pool.bestEffortGetNumOfUsedBuffers()) / pool.getNumBuffers();
	}

	private BufferFileWriter createNewWriter() {
		BufferFileWriter writer = null;
		try {
			writer = ioManager.createBufferFileWriter(ioManager.createChannel(), CALLBACK);
		} catch (IOException e) {
			throw new RuntimeException("Failed to create BufferFileWriter. Reason: " + e.getMessage());
		}
		return writer;
	}

	public boolean hasFullUnspilledEpoch() {
		synchronized (flushLock) {
			if (slicedLog.isEmpty())
				return false;

			for (Map.Entry<Long, Epoch> entry : slicedLog.entrySet()) {
				if (!entry.getKey().equals(slicedLog.lastKey()))
					if (entry.getValue().hasNeverBeenFlushed())
						return true;
			}
		}

		return false;
	}

	static class Epoch {
		private final List<Buffer> epochBuffers;
		private final BufferFileWriter writer;
		private int nextBufferToFlush;
		private int lastBufferFlushed;

		public Epoch(BufferFileWriter writer) {
			this.epochBuffers = new ArrayList<>(50);
			this.writer = writer;
			this.nextBufferToFlush = 0;
			this.lastBufferFlushed = -1;
		}

		public void append(Buffer buffer) {
			this.epochBuffers.add(buffer.retainBuffer());
		}

		public List<Buffer> getEpochBuffers() {
			return epochBuffers;
		}

		public FileIOChannel.ID getFileHandle() {
			return writer.getChannelID();
		}

		public void flushAllUnflushed() {
			for (int i = nextBufferToFlush; i < epochBuffers.size(); i++)
				flushNext();
		}

		public void flushNext() {
			try {
				writer.writeBlock(epochBuffers.get(nextBufferToFlush++));
			} catch (IOException e) {
				throw new RuntimeException("Writer could not write buffer. Cause:" + e.getMessage());
			}
		}

		public void retryLastFlush() {
			try {
				writer.writeBlock(epochBuffers.get(lastBufferFlushed + 1));
			} catch (IOException e) {
				throw new RuntimeException("Writer could not write buffer. Cause:" + e.getMessage());
			}
		}

		public void notifyFlushCompleted() {
			epochBuffers.set(++lastBufferFlushed, null).recycleBuffer();
		}


		public boolean fullyFlushed() {
			return lastBufferFlushed + 1 == epochBuffers.size();
		}

		public void removeEpochFile() {
			//TODO Blocks while open requests. Possibly may need to push this to an executor for performance.
			try {
				writer.closeAndDelete();
				for (int i = nextBufferToFlush; i < epochBuffers.size(); i++) {
					LOG.info("Released buffer {}/{} manually", i, epochBuffers.size());
					epochBuffers.get(i).recycleBuffer(); // release the buffers left over
				}
			} catch (IOException e) {
				throw new RuntimeException("Could not close and delete epoch. Cause: " + e.getMessage());
			}
		}


		public boolean hasNeverBeenFlushed() {
			return nextBufferToFlush == 0;
		}
	}

	private class FlushCompletedCallback implements RequestDoneCallback<Buffer> {

		private long computeNextKey(SortedMap<Long, Epoch> slicedLog, long currentAsyncStoreEpoch) {
			Set<Long> keys = slicedLog.keySet();
			boolean foundKey = false;
			for (Long key : keys) {
				if (foundKey)
					return key; //If we already found our key, this is the next one.
				if (key == currentAsyncStoreEpoch)
					foundKey = true;
			}
			//If no key found return -1, on next append, currentAsyncStoreEpoch will be set.
			return -1;
		}

		@Override
		public void requestSuccessful(Buffer request) {
			LOG.info("Flush completed");
			synchronized (flushLock) {
				Epoch epoch = slicedLog.get(currentAsyncStoreEpoch);
				epoch.notifyFlushCompleted();
				if (epoch.fullyFlushed()) {
					currentAsyncStoreEpoch = computeNextKey(slicedLog, currentAsyncStoreEpoch);
				}
			}
		}

		@Override
		public void requestFailed(Buffer buffer, IOException e) {
			LOG.info("Flush failed. Retrying. Cause: {}", e.getMessage());
			synchronized (flushLock) {
				Epoch epoch = slicedLog.get(currentAsyncStoreEpoch);
				epoch.retryLastFlush();
			}
		}
	}


}
