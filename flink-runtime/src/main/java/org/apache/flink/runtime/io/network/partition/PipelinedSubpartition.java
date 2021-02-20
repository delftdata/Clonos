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

import org.apache.flink.runtime.causal.recovery.IRecoveryManager;
import org.apache.flink.runtime.inflightlogging.*;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pipelined in-memory only subpartition, which can be consumed once.
 *
 * <p>Whenever {@link #add(BufferConsumer, boolean)} adds a finished {@link BufferConsumer} or a second
 * {@link BufferConsumer} (in which case we will assume the first one finished), we will
 * {@link PipelinedSubpartitionView#notifyDataAvailable() notify} a read view created via
 * {@link #createReadView(BufferAvailabilityListener)} of new data availability. Except by calling
 * {@link #flush()} explicitly, we always only notify when the first finished buffer turns up and
 * then, the reader has to drain the buffers via {@link #pollBuffer()} until its return value shows
 * no more buffers being available. This results in a buffer queue which is either empty or has an
 * unfinished {@link BufferConsumer} left from which the notifications will eventually start again.
 *
 * <p>Explicit calls to {@link #flush()} will force this
 * {@link PipelinedSubpartitionView#notifyDataAvailable() notification} for any
 * {@link BufferConsumer} present in the queue.
 */
public class PipelinedSubpartition extends ResultSubpartition {

	private static final Logger LOG = LoggerFactory.getLogger(PipelinedSubpartition.class);

	// ------------------------------------------------------------------------

	/**
	 * The read view to consume this subpartition.
	 */
	private PipelinedSubpartitionView readView;

	/**
	 * Flag indicating whether the subpartition has been finished.
	 */
	private boolean isFinished;

	@GuardedBy("buffers")
	private boolean flushRequested;

	/**
	 * Flag indicating whether the subpartition has been released.
	 */
	private volatile boolean isReleased;
	// ------------------------------------------------------------------------

	private final InFlightLog inFlightLog;
	private IRecoveryManager recoveryManager;

	private final AtomicBoolean downstreamFailed;

	@GuardedBy("buffers")
	private InFlightLogIterator<Buffer> inflightReplayIterator;


	private final AtomicBoolean isRecoveringSubpartitionInFlightState;


	private short vertexID;

	PipelinedSubpartition(int index, ResultPartition parent, InFlightLog inFlightLog) {
		super(index, parent);
		this.inFlightLog = inFlightLog;
		this.downstreamFailed = new AtomicBoolean(false);
		this.isRecoveringSubpartitionInFlightState = new AtomicBoolean(false);
	}


	public void setRecoveryManager(IRecoveryManager recoveryManager) {
		this.recoveryManager = recoveryManager;
	}

	public InFlightLog getInFlightLog() {
		return inFlightLog;
	}

	public void notifyDownstreamCheckpointComplete(int numBuffersProcessedDownstream) {
		synchronized (buffers) {
			inFlightLog.notifyDownstreamCheckpointComplete(numBuffersProcessedDownstream);
		}
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
			// if there is more then 1 buffer, we already notified the reader
			// (at the latest when adding the second buffer)
			flushRequested = !buffers.isEmpty();
			if (!isRecoveringSubpartitionInFlightState.get())
				notifyDataAvailable();
		}
	}

	@Override
	public void finish() throws IOException {
		add(EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE), true);
		LOG.debug("{}: Finished {}.", parent.getOwningTaskName(), this);
	}

	private boolean add(BufferConsumer bufferConsumer, boolean finish) {
		checkNotNull(bufferConsumer);

		final boolean notifyDataAvailable;
		synchronized (buffers) {
			if (isFinished || isReleased) {
				bufferConsumer.close();
				return false;
			}

			// Add the bufferConsumer and update the stats
			buffers.add(bufferConsumer);
			updateStatistics(bufferConsumer);
			increaseBuffersInBacklog(bufferConsumer);
			notifyDataAvailable = shouldNotifyDataAvailable() || finish;

			isFinished |= finish;

			if (isRecoveringSubpartitionInFlightState.get())
				buffers.notifyAll();
			else if (downstreamFailed.get() || inflightReplayIterator != null)
				sendFinishedBuffersToInFlightLog();

		}

		if (notifyDataAvailable && !isRecoveringSubpartitionInFlightState.get()) {
			notifyDataAvailable();
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

			//inFlightLog.close();
			//if (inflightReplayIterator != null)
			//	inflightReplayIterator.close();


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

		LOG.debug("{}: Released {}.", parent.getOwningTaskName(), this);

		if (view != null) {
			view.releaseAllResources();
		}
	}

	public void sendFailConsumerTrigger(Throwable cause) {
		LOG.debug("Sending fail consumer trigger for Partition {} subpartition {} ", parent.getPartitionId(), index);
		downstreamFailed.set(true);

		synchronized (buffers) {
			//If we are  not ourselves recovering
			if (!isRecoveringSubpartitionInFlightState.get())
				sendFinishedBuffersToInFlightLog();
		}

		parent.sendFailConsumerTrigger(index, cause);
	}

	private void sendFinishedBuffersToInFlightLog() {
		synchronized (buffers) {
			while (buffers.size() > 1) {
				//Send the buffer through the inflight log
				BufferAndBacklog bnb = getBufferFromQueuedBufferConsumersUnsafe();
				if (bnb != null)
					bnb.buffer().recycleBuffer();

			}
		}
	}

	@Nullable
	BufferAndBacklog pollBuffer() {

		if (downstreamFailed.get()) {
			LOG.debug("Polling for next buffer, but downstream is still failed.");
			return null;
		}

		if (isRecoveringSubpartitionInFlightState.get()) {
			LOG.debug("We are still recovering this subpartition, cannot return a buffer yet.");
			return null;
		}

		BufferAndBacklog buf;
		synchronized (buffers) {
				LOG.debug("We are not replaying index {}, get buffer from consumers", index);
				buf = getBufferFromQueuedBufferConsumersUnsafe();
		}
		return buf;

	}

	private BufferAndBacklog getBufferFromQueuedBufferConsumersUnsafe() {
		Buffer buffer = null;
		boolean isFinished = false;
		long epochID = 0;

		if (buffers.isEmpty()) {
			flushRequested = false;
		}

		if (buffers.isEmpty())
			LOG.debug("Call to getBufferFromQueued, but no buffer consumers to close");
		while (!buffers.isEmpty()) {
			BufferConsumer bufferConsumer = buffers.peek();

			buffer = bufferConsumer.build();

			checkState(bufferConsumer.isFinished() || buffers.size() == 1,
				"When there are multiple buffers, an unfinished bufferConsumer can not be at the head of the " +
					"buffers queue.");

			if (buffers.size() == 1) {
				// turn off flushRequested flag if we drained all of the available data
				flushRequested = false;
			}

			if (bufferConsumer.isFinished()) {
				isFinished = true;
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

		//subpartitionThreadCausalLog.appendDeterminant(reuseBufferBuiltDeterminant.replace(buffer.readableBytes())
		//	, epochID);
		inFlightLog.log(buffer, isFinished);

		updateStatistics(buffer);
		BufferAndBacklog result = new BufferAndBacklog(buffer, isAvailableUnsafe(), getBuffersInBacklog(),
			nextBufferIsEventUnsafe(), epochID);
		// Do not report last remaining buffer on buffers as available to read (assuming it's unfinished).
		// It will be reported for reading either on flush or when the number of buffers in the queue
		// will be 2 or more.
		if (LOG.isDebugEnabled())
			LOG.debug("{}:{}: Polled buffer {} (hash: {}, memorySegment hash: {}). Buffers available for dispatch: {}."
				, parent, this, buffer, System.identityHashCode(buffer),
				System.identityHashCode(buffer.getMemorySegment()), getBuffersInBacklog());
		return result;
	}


	boolean nextBufferIsEvent() {
		synchronized (buffers) {
			return nextBufferIsEventUnsafe();
		}
	}

	private boolean nextBufferIsEventUnsafe() {
		assert Thread.holdsLock(buffers);
		if (inflightReplayIterator != null)
			return !inflightReplayIterator.peekNext().isBuffer();
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
				LOG.debug("Creating read view for {} (index: {}) of partition {}.", this, index,
					parent.getPartitionId());

				readView = new PipelinedSubpartitionView(this, availabilityListener);
			} else {
				readView.setAvailabilityListener(availabilityListener);
				LOG.debug("(Re)using read view {} for {} (index: {}) of partition {}.", readView, this, index,
					parent.getPartitionId());
			}


		}
		//If we are recovering, when we conclude, we must notify of data availability.
		if (recoveryManager == null || !recoveryManager.isRecovering()) {
			notifyDataAvailable();
		} else {
			recoveryManager.notifyNewOutputChannel(parent.getPartitionId().getPartitionId(), index);

		}

		return readView;
	}

	public boolean isAvailable() {
		boolean result;
		synchronized (buffers) {
			result = isAvailableUnsafe();
		}
		return result;
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
			"PipelinedSubpartition#%d [number of buffers: %d (%d bytes), number of buffers in backlog: %d, finished? %s, read view? %s]",
			index, numBuffers, numBytes, getBuffersInBacklog(), finished, hasReadView);
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		// since we do not synchronize, the size may actually be lower than 0!
		return Math.max(buffers.size() + (inflightReplayIterator != null ? inflightReplayIterator.numberRemaining() :
			0), 0);
	}

	public void requestReplay() {
		LOG.debug("Replay requested");
		synchronized (buffers) {
			if (inflightReplayIterator != null)
				inflightReplayIterator.close();
			inflightReplayIterator = inFlightLog.getInFlightIterator();
			if (inflightReplayIterator != null) {
				if (!inflightReplayIterator.hasNext())
					inflightReplayIterator = null;
			}
			downstreamFailed.set(false);
		}
	}


	private boolean shouldNotifyDataAvailable() {
		// Notify only when we added first finished buffer.
		return readView != null && !flushRequested && getNumberOfFinishedBuffers() == 1;
	}

	public void notifyDataAvailable() {
		if (readView != null) {
			readView.notifyDataAvailable();
		}
	}

	private int getNumberOfFinishedBuffers() {
		assert Thread.holdsLock(buffers);

		// NOTE: isFinished() is not guaranteed to provide the most up-to-date state here
		// worst-case: a single finished buffer sits around until the next flush() call
		// (but we do not offer stronger guarantees anyway)
		if (buffers.size() == 1 && buffers.peekLast().isFinished()) {
			if (inflightReplayIterator != null)
				return 1 + inflightReplayIterator.numberRemaining();
			return 1;
		}

		// We assume that only last buffer is not finished.
		return Math.max(0, buffers.size() + (inflightReplayIterator != null ?
			inflightReplayIterator.numberRemaining() : 0) - 1);
	}

	public void buildAndLogBuffer(int bufferSize) throws InterruptedException {
		LOG.debug("building buffer of size {} and discarding result", bufferSize);
		synchronized (buffers) {
			while (true) {
				BufferConsumer consumer = buffers.peek();

				//No consumer ready
				if (consumer == null) {
					buffers.wait(5);
					continue;
				}

				//Empty consumer (No dataflow between checkpoints)
				if (consumer.isFinished() && consumer.getUnreadBytes() <= 0) {
					buffers.pop().close();
					continue;
				}

				// Erroneous state, consumer is finished without enough data, throw exception
				if (consumer.isFinished() && consumer.getUnreadBytes() > 0 && consumer.getUnreadBytes() < bufferSize) {
					String msg = "Vertex " + recoveryManager.getContext().getTaskVertexID() + " - Size of finished bufferConsumer ( unread: " + consumer.getUnreadBytes() +
						", written: " + consumer.getWrittenBytes() +
						") does not match size of recovery request to build buffer ( " + bufferSize + " ).";
					LOG.info("Exception:" + msg);
					throw new RuntimeException(msg);
				}
				//If there is enough data in consumer for building the correct buffer
				if (consumer.getUnreadBytes() >= bufferSize) {
					buildRequestedBuffer(bufferSize, consumer);
					break;
				}

				buffers.wait(5);
			}
		}
		LOG.debug("Done building and discarding bufer of size {}", bufferSize);
	}

	private void buildRequestedBuffer(int bufferSize, BufferConsumer consumer) {
		LOG.debug("There are enough bytes to build the requested buffer!");

		//This assumes that the input buffers which are before this close in the determinant log have
		// been
		// fully processed, thus the bufferconsumer will have this amount of data.
		//subpartitionThreadCausalLog.appendDeterminant(reuseBufferBuiltDeterminant.replace(bufferSize), epochID);
		Buffer buffer = consumer.build(bufferSize);


		checkState(consumer.isFinished() || buffers.size() == 1,
			"When there are multiple buffers, an unfinished bufferConsumer can not be at the head of" +
				" " +
				"the buffers queue.");

		if (buffers.size() == 1) {
			// turn off flushRequested flag if we drained all of the available data
			flushRequested = false;
		}


		updateStatistics(buffer);
		inFlightLog.log(buffer, true);
		buffer.recycleBuffer(); //It is not sent downstream, so we must recycle it here.
	}

	@Override
	public short getVertexID() {
		return vertexID;
	}

	public boolean isRecoveringSubpartititionInFlightState() {
		return isRecoveringSubpartitionInFlightState.get();
	}
}
