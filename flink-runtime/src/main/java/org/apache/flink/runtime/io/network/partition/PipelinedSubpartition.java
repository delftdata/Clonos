/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.inflightlogging.InFlightLog;
import org.apache.flink.runtime.inflightlogging.SizedListIterator;
import org.apache.flink.runtime.inflightlogging.SubpartitionInFlightLogger;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pipelined in-memory only subpartition, which can be consumed once.
 */
class PipelinedSubpartition extends ResultSubpartition {

	private static final Logger LOG = LoggerFactory.getLogger(PipelinedSubpartition.class);

	// ------------------------------------------------------------------------

	/** The read view to consume this subpartition. */
	private PipelinedSubpartitionView readView;

	/** Flag indicating whether the subpartition has been finished. */
	private boolean isFinished;

	@GuardedBy("buffers")
	private boolean flushRequested;

	/** Flag indicating whether the subpartition has been released. */
	private volatile boolean isReleased;
	// ------------------------------------------------------------------------

	private InFlightLog inFlightLog;

	private long nextCheckpointId;

	/**
	 * Access is also guarded by buffers lock
	 * -1 represents not a checkpoint buffer
	 */
	private Deque<Long> checkpointIds;

	private AtomicBoolean isPreparedToReplay;
	private AtomicBoolean isReplaying;

	private SizedListIterator<Buffer> replayIterator;

	PipelinedSubpartition(int index, ResultPartition parent) {
		super(index, parent);

		this.inFlightLog = new SubpartitionInFlightLogger();
		this.checkpointIds = new LinkedList<>();
		this.nextCheckpointId = -1l;
		this.isReplaying.set(false);
	}

	public void notifyCheckpointBarrier(long checkpointId){
		this.nextCheckpointId = checkpointId;
	}

	public void notifyCheckpointComplete(long checkpointId){
		this.inFlightLog.notifyCheckpointComplete(checkpointId);
	}

	@Override
	public boolean add(BufferConsumer bufferConsumer) {
		return add(bufferConsumer, false);
	}

	@Override
	public void flush() {
		synchronized (buffers) {
			if (buffers.isEmpty()) {
				return;
			}
			flushRequested = !buffers.isEmpty();
			notifyDataAvailable();
		}
	}

	@Override
	public void finish() throws IOException {
		add(EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE), true);
		LOG.debug("Finished {}.", this);
	}

	private boolean add(BufferConsumer bufferConsumer, boolean finish) {
		checkNotNull(bufferConsumer);

		synchronized (buffers) {
			if (isFinished || isReleased) {
				bufferConsumer.close();
				return false;
			}

			// Add the bufferConsumer and update the stats
			buffers.add(bufferConsumer);
			updateStatistics(bufferConsumer);
			increaseBuffersInBacklog(bufferConsumer);

			checkpointIds.push(nextCheckpointId);
			//todo - possible issue if two checkpoints in a row
			nextCheckpointId = -1;

			if (finish) {
				isFinished = true;
				flush();
			}
			else {
				maybeNotifyDataAvailable();
			}
		}

		return true;
	}

	@Override
	public void release() {
		// view reference accessible outside the lock, but assigned inside the locked scope
		final PipelinedSubpartitionView view;

		synchronized (buffers) {
			if (isReleased) {
				return;
			}

			// Release all available buffers
			for (BufferConsumer buffer : buffers) {
				buffer.close();
			}
			buffers.clear();



			view = readView;
			readView = null;

			// Make sure that no further buffers are added to the subpartition
			isReleased = true;
		}

		LOG.debug("Released {}.", this);

		if (view != null) {
			view.releaseAllResources();
		}
	}

	// Release all available buffers. Needed in RunStandbyTaskStrategy for producer whose consumer failed.
	public void releaseBuffers() {
		// Release all available buffers
		synchronized (buffers) {
			LOG.debug("Release {} buffers of {}.", buffers.size(), this);
			for (BufferConsumer buffer : buffers) {
				buffer.close();
			}
			buffers.clear();
			resetBuffersInBacklog();
			LOG.debug("Released buffers of {}. Now {} buffers.", this, buffers.size());
		}
	}

	public void sendFailConsumerTrigger(Throwable cause) {
		parent.sendFailConsumerTrigger(index, cause);
	}

	@Nullable
	BufferAndBacklog pollBuffer() {
		while(isPreparedToReplay.get()); //wait

		if(isReplaying.get())
			return getReplayedBuffer();
		else
			return getBufferFromQueuedBufferConsumers();

	}

	private BufferAndBacklog getReplayedBuffer() {
		Buffer buffer = replayIterator.next();
		if(!replayIterator.hasNext())
			isReplaying.set(false);
		return new BufferAndBacklog(buffer,
			replayIterator.hasNext() || isAvailableUnsafe(),
			getBuffersInBacklog() + replayIterator.numberRemaining(),
			_recoveryNextBufferIsEvent());
	}

	private boolean _recoveryNextBufferIsEvent() {
		boolean isNextAnEvent;
		if(replayIterator.hasNext()){
			isNextAnEvent = !replayIterator.next().isBuffer();
			replayIterator.previous(); //return to previous position
		}else
			isNextAnEvent = _nextBufferIsEvent();
		return isNextAnEvent;
	}

	private BufferAndBacklog getBufferFromQueuedBufferConsumers() {
		synchronized (buffers) {
			Buffer buffer = null;

			if (buffers.isEmpty()) {
				flushRequested = false;
			}

			while (!buffers.isEmpty()) {
				BufferConsumer bufferConsumer = buffers.peek();

				buffer = bufferConsumer.build();

				logBuffer(buffer);

				checkState(bufferConsumer.isFinished() || buffers.size() == 1,
					"When there are multiple buffers, an unfinished bufferConsumer can not be at the head of the buffers queue.");

				if (buffers.size() == 1) {
					// turn off flushRequested flag if we drained all of the available data
					flushRequested = false;
				}

				if (bufferConsumer.isFinished()) {
					buffers.pop().close();
					decreaseBuffersInBacklogUnsafe(bufferConsumer.isBuffer());
				}

				if (buffer.readableBytes() > 0) {
					break;
				}
				buffer.recycleBuffer();
				buffer = null;
				if (!bufferConsumer.isFinished()) {
					break;
				}
			}

			if (buffer == null) {
				return null;
			}

			updateStatistics(buffer);
			// Do not report last remaining buffer on buffers as available to read (assuming it's unfinished).
			// It will be reported for reading either on flush or when the number of buffers in the queue
			// will be 2 or more.
			LOG.debug("{}:{}: Polled buffer {} (hash: {}, memorySegment hash: {}). Buffers available for dispatch: {}.", parent, this, buffer, System.identityHashCode(buffer), System.identityHashCode(buffer.getMemorySegment()), getBuffersInBacklog());
			return new BufferAndBacklog(
				buffer,
				isAvailableUnsafe(),
				getBuffersInBacklog(),
				_nextBufferIsEvent());
		}
	}

	private void logBuffer(Buffer buffer) {
		long checkpointId = checkpointIds.pop();
		boolean isCheckpoint = checkpointId != -1L;
		if(isCheckpoint)
			inFlightLog.logCheckpointBarrier(buffer, checkpointId);
		else
			inFlightLog.log(buffer);
	}

	boolean nextBufferIsEvent() {
		synchronized (buffers) {
			return _nextBufferIsEvent();
		}
	}

	private boolean _nextBufferIsEvent() {
		assert Thread.holdsLock(buffers);

		return !buffers.isEmpty() && !buffers.peekFirst().isBuffer();
	}

	@Override
	public int releaseMemory() {
		// The pipelined subpartition does not react to memory release requests.
		// The buffers will be recycled by the consuming task.
		return 0;
	}

	@Override
	public boolean isReleased() {
		return isReleased;
	}

	@Override
	public PipelinedSubpartitionView createReadView(BufferAvailabilityListener availabilityListener) throws IOException {
		synchronized (buffers) {
			checkState(!isReleased);

			if (readView == null) {
				LOG.debug("Creating read view for {} (index: {}) of partition {}.", this, index, parent.getPartitionId());

				readView = new PipelinedSubpartitionView(this, availabilityListener);
			} else {
				readView.setAvailabilityListener(availabilityListener);
				LOG.debug("(Re)using read view {} for {} (index: {}) of partition {}.", readView, this, index, parent.getPartitionId());
			}

			if (!buffers.isEmpty()) {
				notifyDataAvailable();
			}
		}

		return readView;
	}

	public boolean isAvailable() {
		synchronized (buffers) {
			return isAvailableUnsafe();
		}
	}

	private boolean isAvailableUnsafe() {
		return flushRequested || getNumberOfFinishedBuffers() > 0;
	}

	// ------------------------------------------------------------------------

	int getCurrentNumberOfBuffers() {
		return buffers.size();
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		final long numBuffers;
		final long numBytes;
		final boolean finished;
		final boolean hasReadView;

		synchronized (buffers) {
			numBuffers = getTotalNumberOfBuffers();
			numBytes = getTotalNumberOfBytes();
			finished = isFinished;
			hasReadView = readView != null;
		}

		return String.format(
			"PipelinedSubpartition %d [number of buffers: %d (%d bytes), number of buffers in backlog: %d, finished? %s, read view? %s]",
			index, numBuffers, numBytes, getBuffersInBacklog(), finished, hasReadView);
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		// since we do not synchronize, the size may actually be lower than 0!
		return Math.max(buffers.size(), 0);
	}

	private void maybeNotifyDataAvailable() {
		// Notify only when we added first finished buffer.
		if (getNumberOfFinishedBuffers() == 1) {
			notifyDataAvailable();
		}
	}

	private void notifyDataAvailable() {
		if (readView != null) {
			readView.notifyDataAvailable();
		}
	}

	private int getNumberOfFinishedBuffers() {
		assert Thread.holdsLock(buffers);

		if (buffers.size() == 1 && buffers.peekLast().isFinished()) {
			return 1;
		}

		// We assume that only last buffer is not finished.
		return Math.max(0, buffers.size() - 1);
	}

	public void prepareInFlightReplay(long checkpointId) {
			isPreparedToReplay.set(true);			
			replayIterator = inFlightLog.getInFlightFromCheckpoint(checkpointId);
	}

	public void startInFlightReplay(long checkpointId) {
		isPreparedToReplay.set(false);
		isReplaying.set(true);
	}
}
