/*
 *
 *
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional debugrmation
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

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

	private final Consumer<SpillableSubpartitionInFlightLogger> flushPolicy;

	private final Object subpartitionLock = new Object();
	private BufferPool partitionBufferPool;

	private float availabilityFillFactor;

	public SpillableSubpartitionInFlightLogger(IOManager ioManager,
											   Consumer<SpillableSubpartitionInFlightLogger> flushPolicy) {
		this(ioManager, flushPolicy, 0.5f);
	}

	public SpillableSubpartitionInFlightLogger(IOManager ioManager,
											   Consumer<SpillableSubpartitionInFlightLogger> flushPolicy,
											   float availabilityFillFactor) {
		this.ioManager = ioManager;
		this.flushPolicy = flushPolicy;
		this.availabilityFillFactor = availabilityFillFactor;

		slicedLog = new TreeMap<>();
	}

	@Override
	public void log(Buffer buffer, long epochID) {
		synchronized (subpartitionLock) {
			Epoch epoch = slicedLog.computeIfAbsent(epochID, k -> new Epoch(createNewWriter(k), k));
			//If we have fully flushed this epoch and there is a next epoch
			epoch.append(buffer);
			flushPolicy.accept(this);
		}
		LOG.debug("Logged a new buffer for epoch {}", epochID);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointID) throws Exception {
		LOG.debug("Got notified of checkpoint {} completion", checkpointID);
		List<Long> toRemove = new LinkedList<>();

		synchronized (subpartitionLock) {
			//keys are in ascending order
			for (long epochID : slicedLog.keySet()) {
				if (epochID < checkpointID) {
					toRemove.add(epochID);
					LOG.debug("Removing epoch {}", epochID);
				}
			}
			for (long epochID : toRemove)
				slicedLog.remove(epochID).removeEpochFile();
		}
	}

	@Override
	public InFlightLogIterator<Buffer> getInFlightIterator(long epochID, int ignoreBuffers) {
		SortedMap<Long, Epoch> logToReplay;
		synchronized (subpartitionLock) {
			logToReplay = slicedLog.tailMap(epochID);
			if (logToReplay.size() == 0)
				return null;
		}

		return new SpilledReplayIterator(logToReplay, partitionBufferPool, ioManager, subpartitionLock, ignoreBuffers);
	}

	@Override
	public void registerBufferPool(BufferPool bufferPool) {
		this.partitionBufferPool = bufferPool;
	}

	public SortedMap<Long, Epoch> getSlicedLog() {
		return slicedLog;
	}

	private void notifyFlushCompleted(long epochID) {
		synchronized (subpartitionLock) {
			Epoch epoch = slicedLog.get(epochID);
			if (epoch != null && !epoch.stable())
				epoch.notifyFlushCompleted();
		}
	}

	private void notifyFlushFailed(long epochID) {
		synchronized (subpartitionLock) {
			Epoch epoch = slicedLog.get(epochID);
			if (epoch != null && !epoch.stable())
				epoch.notifyFlushFailed();
		}
	}


	public boolean isPoolAvailabilityLow() {
		return computePoolAvailability() < availabilityFillFactor;
	}

	private float computePoolAvailability() {
		return 1 - ((float) partitionBufferPool.bestEffortGetNumOfUsedBuffers()) / partitionBufferPool.getNumBuffers();
	}

	private BufferFileWriter createNewWriter(long epochID) {
		BufferFileWriter writer = null;
		try {
			writer = ioManager.createBufferFileWriter(ioManager.createChannel(), new FlushCompletedCallback(this,
				epochID));
		} catch (IOException e) {
			throw new RuntimeException("Failed to create BufferFileWriter. Reason: " + e.getMessage());
		}
		return writer;
	}

	static class Epoch {
		private final List<BufferHandle> epochBuffers;
		private final BufferFileWriter writer;
		private int nextBufferToFlush;
		private int nextBufferToCompleteFlushing;
		private long epochID;


		public Epoch(BufferFileWriter writer, long epochID) {
			this.epochBuffers = new ArrayList<>(50);
			this.writer = writer;
			this.nextBufferToFlush = 0;
			this.nextBufferToCompleteFlushing = 0;
			this.epochID = epochID;
		}

		public void append(Buffer buffer) {
			this.epochBuffers.add(new BufferHandle(buffer.retainBuffer()));
		}

		public List<BufferHandle> getEpochBuffers() {
			return epochBuffers;
		}

		public FileIOChannel.ID getFileHandle() {
			return writer.getChannelID();
		}

		public long getEpochID() {
			return epochID;
		}

		public void flushAllUnflushed() {
			for (int i = nextBufferToFlush; i < epochBuffers.size(); i++) {
				LOG.debug("Flushing buffer {} of epoch {}. Buffer:{}", nextBufferToFlush, epochID,
					epochBuffers.get(nextBufferToFlush));
				flushBuffer(epochBuffers.get(nextBufferToFlush));
				nextBufferToFlush++;
			}
		}

		public void flushBuffer(BufferHandle bufferHandle) {
			try {
				if (!writer.isClosed())
					writer.writeBlock(bufferHandle.getBuffer());
			} catch (IOException e) {
				throw new RuntimeException("Writer could not write buffer. Cause:" + e.getMessage());
			}
		}

		public void notifyFlushCompleted() {
			LOG.debug("Flush completed for buffer {} of epoch {}, recycling", nextBufferToCompleteFlushing, epochID);
			BufferHandle handle = epochBuffers.get(nextBufferToCompleteFlushing);
			handle.markFlushed();
			handle.getBuffer().recycleBuffer();
			nextBufferToCompleteFlushing++;
		}

		public void notifyFlushFailed() {
			//Must clear request queue, otherwise buffers are stored in wrong order
			writer.clearRequestQueue();
			//Resubmit requests in order
			if (!writer.isClosed())
				for (int i = nextBufferToCompleteFlushing; i < nextBufferToFlush; i++) {
					try {
						writer.writeBlock(epochBuffers.get(i).getBuffer());
					} catch (IOException e) {
						throw new RuntimeException("Writer could not write buffer. Cause:" + e.getMessage());
					}
				}

		}

		public boolean stable() {
			return nextBufferToCompleteFlushing == epochBuffers.size() || writer.isClosed();
		}

		public void removeEpochFile() {
			LOG.debug("Removing epoch file of epoch {}", epochID);
			//TODO Blocks while open requests. Possibly may need to push this to an executor for performance.
			try {
				writer.closeAndDelete();
				for (int i = nextBufferToFlush; i < epochBuffers.size(); i++) {
					LOG.debug("Released buffer {}/{} manually", i, epochBuffers.size());
					epochBuffers.get(i).getBuffer().recycleBuffer(); // release the buffers left over
				}
			} catch (IOException e) {
				throw new RuntimeException("Could not close and delete epoch. Cause: " + e.getMessage());
			}
		}


		public boolean hasNeverBeenFlushed() {
			return nextBufferToFlush == 0;
		}

		public int getEpochSize() {
			return epochBuffers.size();
		}

		@Override
		public String toString() {
			return "Epoch{" +
				"size=" + epochBuffers.size() +
				",nextBufferToFlush=" + nextBufferToFlush +
				", nextBufferToCompleteFlushing=" + nextBufferToCompleteFlushing +
				'}';
		}
	}

	static class BufferHandle {
		private Buffer buffer;
		private boolean flushed;
		private boolean availableInMemory;

		public BufferHandle(Buffer buffer) {
			this.buffer = buffer;
			this.availableInMemory = true;
			this.flushed = false;
		}

		public Buffer getBuffer() {
			return buffer;
		}

		public boolean isFlushed() {
			return flushed;
		}

		public boolean isAvailableInMemory() {
			return availableInMemory;
		}

		public void markFlushed() {
			this.flushed = true;
			this.availableInMemory = false;
		}
	}

	private static class FlushCompletedCallback implements RequestDoneCallback<Buffer> {

		private final SpillableSubpartitionInFlightLogger toNotify;
		private final long epochID;

		public FlushCompletedCallback(SpillableSubpartitionInFlightLogger toNotify, long epochID) {
			this.epochID = epochID;
			this.toNotify = toNotify;
		}

		@Override
		public void requestSuccessful(Buffer request) {
			toNotify.notifyFlushCompleted(epochID);
		}

		@Override
		public void requestFailed(Buffer buffer, IOException e) {
			LOG.debug("Flush failed. Retrying. Cause: {}", e.getMessage());
			toNotify.notifyFlushFailed(epochID);
		}
	}


}
