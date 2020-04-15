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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.causal.*;
import org.apache.flink.runtime.causal.determinant.RNGDeterminant;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.InFlightLogPrepareEvent;
import org.apache.flink.runtime.event.InFlightLogRequestEvent;
import org.apache.flink.runtime.inflightlogging.InFlightLogger;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.XORShiftRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.apache.flink.runtime.io.network.api.serialization.RecordSerializer.SerializationResult;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A record-oriented runtime result writer.
 *
 * <p>The RecordWriter wraps the runtime's {@link ResultPartitionWriter} and takes care of
 * serializing records into buffers.
 *
 * <p><strong>Important</strong>: it is necessary to call {@link #flushAll()} after
 * all records have been written with {@link #emit(IOReadableWritable)}. This
 * ensures that all produced records are written to the output stream (incl.
 * partially filled ones).
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public class RecordWriter<T extends IOReadableWritable> {

	private static final Logger LOG = LoggerFactory.getLogger(RecordWriter.class);

	protected final ResultPartitionWriter targetPartition;

	protected final ChannelSelector<T> channelSelector;

	protected final int numChannels;

	/**
	 * {@link RecordSerializer} per outgoing channel.
	 */
	protected final RecordSerializer<T>[] serializers;

	protected final Optional<BufferBuilder>[] bufferBuilders;

	protected final Random rng = new XORShiftRandom();

	protected final boolean flushAlways;

	protected Counter numBytesOut = new SimpleCounter();

	protected final InFlightLogger inFlightLogger;

	protected boolean buffersCleared = false;


	public RecordWriter(ResultPartitionWriter writer) {
		this(writer, new RoundRobinChannelSelector<T>());
	}

	@SuppressWarnings("unchecked")
	public RecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector) {
		this(writer, channelSelector, false);
	}

	public RecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector, boolean flushAlways) {
		this.flushAlways = flushAlways;
		this.targetPartition = writer;
		this.channelSelector = channelSelector;

		this.numChannels = writer.getNumberOfSubpartitions();

		try {
			this.inFlightLogger = new InFlightLogger(this.targetPartition, this.numChannels);
		} catch (IOException e) {
			throw new RuntimeException("Error while creating the RecordWriter.", e);
		}
		/*
		 * The runtime exposes a channel abstraction for the produced results
		 * (see {@link ChannelSelector}). Every channel has an independent
		 * serializer.
		 */
		this.serializers = new SpanningRecordSerializer[numChannels];
		this.bufferBuilders = new Optional[numChannels];
		for (int i = 0; i < numChannels; i++) {
			serializers[i] = new SpanningRecordSerializer<T>();
			bufferBuilders[i] = Optional.empty();
		}
	}


	public void emit(T record) throws IOException, InterruptedException {
		for (int targetChannel : channelSelector.selectChannels(record, numChannels)) {
			sendToTarget(record, targetChannel);
		}
	}

	/**
	 * This is used to broadcast Streaming Watermarks in-band with records. This ignores
	 * the {@link ChannelSelector}.
	 */
	public void broadcastEmit(T record) throws IOException, InterruptedException {
		for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
			sendToTarget(record, targetChannel);
		}
	}

	/**
	 * This is used to send LatencyMarks to a random target channel.
	 */
	public void randomEmit(T record) throws IOException, InterruptedException {
		sendToTarget(record, rng.nextInt(numChannels));
	}

	protected void sendToTarget(T record, int targetChannel) throws IOException, InterruptedException {
		RecordSerializer<T> serializer = serializers[targetChannel];
		LOG.info("RecordWriter send to target");

		SerializationResult result = serializer.addRecord(record);

		if (!inFlightLogger.replaying()) {
			checkReplayInFlightLog();
		}

		while (result.isFullBuffer()) {
			if (tryFinishCurrentBufferBuilder(targetChannel, serializer)) {
				// If this was a full record, we are done. Not breaking
				// out of the loop at this point will lead to another
				// buffer request before breaking out (that would not be
				// a problem per se, but it can lead to stalls in the
				// pipeline).
				if (result.isFullRecord()) {
					break;
				}
			}
			LOG.debug("{}: sendToTarget() calls requestNewBufferBuilder() for resultPartition {}, subpartitionIndex {}.", targetPartition.getTaskName(), targetPartition, targetChannel);
			BufferBuilder bufferBuilder = requestNewBufferBuilder(targetChannel);
			LOG.debug("New BufferBuilder's memory segment hash: {}", bufferBuilder.getMemorySegmentHash());

			result = serializer.continueWritingWithNextBufferBuilder(bufferBuilder);
		}
		checkState(!serializer.hasSerializedData(), "All data should be written at once");

		if (flushAlways) {
			targetPartition.flush(targetChannel);
		}
	}

	public void broadcastEvent(AbstractEvent event) throws IOException, InterruptedException {
		LOG.debug("{}: RecordWriter broadcast event {}.", targetPartition.getTaskName(), event);
		for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
			if (event instanceof CheckpointBarrier)
				inFlightLogger.logCheckpointBarrier(targetChannel, (CheckpointBarrier) event);

			emitEvent(event, targetChannel);
		}
	}

	public void emitEvent(AbstractEvent event, int targetChannel) throws IOException, InterruptedException {
		LOG.debug("{}: RecordWriter replay {}.", targetPartition.getTaskName(), event);

		try (BufferConsumer eventBufferConsumer = EventSerializer.toBufferConsumer(event)) {
			RecordSerializer<T> serializer = serializers[targetChannel];

			tryFinishCurrentBufferBuilder(targetChannel, serializer);

			// retain the buffer so that it can be recycled by each channel of targetPartition
			targetPartition.addBufferConsumer(eventBufferConsumer.copy(), targetChannel);

			if (flushAlways) {
				targetPartition.flush(targetChannel);
			}
		}
	}

	public void flushAll() {
		targetPartition.flushAll();
	}

	public void clearBuffers() throws IOException, InterruptedException {
		LOG.debug("Clear buffers.");
		for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
			RecordSerializer<?> serializer = serializers[targetChannel];
			closeBufferBuilder(targetChannel);
			serializer.clear();
		}
	}

	/**
	 * Sets the metric group for this RecordWriter.
	 */
	public void setMetricGroup(TaskIOMetricGroup metrics) {
		numBytesOut = metrics.getNumBytesOutCounter();
	}

	/**
	 * Marks the current {@link BufferBuilder} as finished and clears the state for next one.
	 *
	 * @return true if some data were written
	 */
	protected boolean tryFinishCurrentBufferBuilder(int targetChannel, RecordSerializer<T> serializer) throws IOException, InterruptedException {

		if (!bufferBuilders[targetChannel].isPresent()) {
			return false;
		}
		BufferBuilder bufferBuilder = bufferBuilders[targetChannel].get();
		bufferBuilders[targetChannel] = Optional.empty();

		numBytesOut.inc(bufferBuilder.finish());
		inFlightLogger.log(bufferBuilder, targetChannel);
		serializer.clear();
		return true;
	}

	private BufferBuilder requestNewBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		checkState(!bufferBuilders[targetChannel].isPresent());
		BufferBuilder bufferBuilder = targetPartition.getBufferProvider().requestBufferBuilderBlocking();
		bufferBuilders[targetChannel] = Optional.of(bufferBuilder);
		targetPartition.addBufferConsumer(bufferBuilder.createBufferConsumer(), targetChannel);
		return bufferBuilder;
	}

	private void closeBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		if (bufferBuilders[targetChannel].isPresent()) {
			BufferBuilder bufferBuilder = bufferBuilders[targetChannel].get();
			bufferBuilders[targetChannel] = Optional.empty();

			LOG.debug("Close bufferbuilder {}.", bufferBuilder);
			numBytesOut.inc(bufferBuilder.finish());
			inFlightLogger.log(bufferBuilder, targetChannel);
		}
	}

	public InFlightLogger getInFlightLogger() {
		return inFlightLogger;
	}

	public void checkReplayInFlightLog() throws IOException, InterruptedException {
		LOG.debug("Check for in-flight log request.");

		if (downstreamFailed() || inFlightLogPrepareSignalled()) {
			InFlightLogPrepareEvent inFlightLogPrepareEvent = getInFlightLogPrepareEvent();
			LOG.info("{} has been signalled. Ack it.", inFlightLogPrepareEvent);
			int subpartitionIndex = inFlightLogPrepareEvent.getSubpartitionIndex();
			long downstreamCheckpointId = inFlightLogPrepareEvent.getCheckpointId();
			if (targetPartition instanceof ResultPartition) {
				((ResultPartition) targetPartition).ackInFlightLogPrepareRequest(subpartitionIndex);
			}

			// Clear the state
			RecordSerializer<T> serializer = serializers[subpartitionIndex];
			if (!buffersCleared) {
				LOG.debug("Close buffer builder.");
				closeBufferBuilder(subpartitionIndex);
				LOG.debug("Clear serializer {}.", serializer);
				serializer.clear();
				LOG.debug("Prune internal state (dataBuffer) of serializer {}.", serializer);
				serializer.prune();
				if (targetPartition instanceof ResultPartition) {
					ResultPartition tp = (ResultPartition) targetPartition;
					LOG.debug("Release buffers of partition {} index {}.", tp, subpartitionIndex);
					tp.releaseBuffers(subpartitionIndex);
				}
				LOG.debug("State of serializer {}.", serializer);
				buffersCleared = true;
			}

			// Wait for replay signal (state restoring downstream)
			if (replaySignalFailed(inFlightLogPrepareEvent)) {
				return;
			}

			int replayCounter = 0;
			int totalReplayCounter = 0;
			TreeSet<Long> checkpointIdsToReplay = inFlightLogger.getCheckpointIdsToReplay(downstreamCheckpointId);
			LOG.info("Received {} checkpoint ids to replay buffers for.", checkpointIdsToReplay.size());

			for (long checkpointId : checkpointIdsToReplay) {
				LOG.debug("Start to replay buffers for checkpoint {}.", checkpointId);
				List<BufferBuilder> bufferBuilders = inFlightLogger.getReplayLog(subpartitionIndex, checkpointId);
				for (BufferBuilder bufferBuilder : bufferBuilders) {
					LOG.debug("{}: Replay buffer at pos {}: {} of task {} for checkpoint {}.", inFlightLogger, replayCounter, bufferBuilder, ((ResultPartition) targetPartition).getTaskName(), checkpointId);
					targetPartition.addBufferConsumer(bufferBuilder.createBufferConsumer(), subpartitionIndex);
					replayCounter++;
					totalReplayCounter++;
				}

				LOG.info("Replaying {} buffers for checkpoint {} completed.", replayCounter, checkpointId);
				replayCounter = 0;

				CheckpointBarrier checkpointBarrier = inFlightLogger.getCheckpointBarrier(subpartitionIndex, checkpointId);
				if (checkpointBarrier != null) {
					LOG.info("Replay {}.", checkpointBarrier);
					emitEvent(checkpointBarrier, subpartitionIndex);
				}
			}
			inFlightLogger.setReplaying(false);

			LOG.info("Replaying {} buffers completed. Flush buffers and check whether there is another in-flight log request.", totalReplayCounter);

			// If flushing each record is disabled,
			// flush the accumulated in-flight log records.
			if (!flushAlways) {
				targetPartition.flush(subpartitionIndex);
			}

			checkReplayInFlightLog();
		}
		buffersCleared = false;
	}

	private boolean inFlightLogRequestSignalled() throws IOException {
		if (targetPartition instanceof ResultPartition) {
			ResultPartition targetResultPartition = (ResultPartition) targetPartition;
			return targetResultPartition.inFlightLogRequestSignalled();
		} else {
			throw new IOException("Unable to check whether in-flight log request is received for partition of type " + targetPartition + ".");
		}
	}

	private boolean downstreamFailed() throws IOException, InterruptedException {
		if (targetPartition instanceof ResultPartition &&
			((ResultPartition) targetPartition).downstreamFailed()) {
			LOG.info("Downstream task of {} failed.", ((ResultPartition) targetPartition).getTaskName());
			int i = 0;
			while (!inFlightLogPrepareSignalled() && i < 100) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				i++;
			}

			if (i == 100) {
				LOG.warn("In-flight log prepare request delayed more than 1 second. Aborting.");
				return false;
			}
			return true;
		}
		return false;
	}

	private boolean inFlightLogPrepareSignalled() throws IOException {
		if (targetPartition instanceof ResultPartition) {
			ResultPartition targetResultPartition = (ResultPartition) targetPartition;
			return targetResultPartition.inFlightLogPrepareSignalled();
		} else {
			throw new IOException("Unable to check whether in-flight log prepare event is received for partition of type " + targetPartition + ".");
		}
	}

	private InFlightLogRequestEvent getInFlightLogRequestEvent() throws IOException {
		if (targetPartition instanceof ResultPartition) {
			ResultPartition targetResultPartition = (ResultPartition) targetPartition;
			return targetResultPartition.getInFlightLogRequestEvent();
		} else {
			throw new IOException("Unable to get in-flight log request event for partition of type " + targetPartition + ".");
		}
	}

	private InFlightLogPrepareEvent getInFlightLogPrepareEvent() throws IOException {
		if (targetPartition instanceof ResultPartition) {
			ResultPartition targetResultPartition = (ResultPartition) targetPartition;
			return targetResultPartition.getInFlightLogPrepareEvent();
		} else {
			throw new IOException("Unable to get in-flight log prepare event for partition of type " + targetPartition + ".");
		}
	}

	private boolean replaySignalFailed(InFlightLogPrepareEvent inFlightLogPrepareEvent) throws IOException, InterruptedException {
		int i = 0;
		while (!inFlightLogRequestSignalled() && i < 100) {
			Thread.sleep(10);
			i++;
		}

		if (i == 100) {
			LOG.warn("In-flight log request {} delayed more than 1 second. Aborting.", inFlightLogPrepareEvent);
			return true;
		}

		InFlightLogRequestEvent inFlightLogRequestEvent = getInFlightLogRequestEvent();
		LOG.debug("{} has been signalled.", inFlightLogRequestEvent);

		if (!inFlightLogRequestEvent.equals(inFlightLogPrepareEvent)) {
			LOG.warn("In-flight log prepare event {} not validated. Replay request {} received afterwards does not match.", inFlightLogPrepareEvent, inFlightLogRequestEvent);
			return true;
		}

		return false;
	}
}
