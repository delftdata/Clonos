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
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.inflightlogging.InFlightLogger;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.util.XORShiftRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.Random;

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

	private final ChannelSelector<T> channelSelector;

	private final int numChannels;

	/**
	 * {@link RecordSerializer} per outgoing channel.
	 */
	private final RecordSerializer<T>[] serializers;

	private final Optional<BufferBuilder>[] bufferBuilders;

	private final Random rng = new XORShiftRandom();

	private final boolean flushAlways;

	private Counter numBytesOut = new SimpleCounter();

	private final InFlightLogger<T, ?> inFlightLogger;

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

		this.inFlightLogger = new InFlightLogger<>(this.numChannels);

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

	private void sendToTarget(T record, int targetChannel) throws IOException, InterruptedException {
		RecordSerializer<T> serializer = serializers[targetChannel];

		SerializationResult result = serializer.addRecord(record);

		if (!inFlightLogger.replaying()) {
			if (record instanceof SerializationDelegate && targetPartition instanceof ResultPartition) {
				LOG.debug("Log record {} of task {} for channel {} to inFLightLogger.", ((SerializationDelegate) record).getInstance(), ((ResultPartition) targetPartition).getTaskName(), targetChannel);
			} else {
				LOG.debug("Log record {} of partition {} for channel {} to inFLightLogger.", record, targetPartition, targetChannel);
			}

			inFlightLogger.logRecord(record, targetChannel);

			//checkReplayInFlightLog();
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
			LOG.debug("{}: sendToTarget() calls requestNewBufferBuilder() for resultPartition {}, targetChannel {}.", targetPartition.getTaskName(), targetPartition, targetChannel);
			BufferBuilder bufferBuilder = requestNewBufferBuilder(targetChannel);
			LOG.debug("New BufferBuilder's memory segment hash: {}", bufferBuilder.getMemorySegmentHash());

			result = serializer.continueWritingWithNextBufferBuilder(bufferBuilder);
		}
		checkState(!serializer.hasSerializedData(), "All data should be written at once");

		if (flushAlways) {
			targetPartition.flush(targetChannel);
		}
	}

	public void broadcastEvent(AbstractEvent event) throws IOException {
		try (BufferConsumer eventBufferConsumer = EventSerializer.toBufferConsumer(event)) {
			for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
				RecordSerializer<T> serializer = serializers[targetChannel];

				tryFinishCurrentBufferBuilder(targetChannel, serializer);

				// retain the buffer so that it can be recycled by each channel of targetPartition
				targetPartition.addBufferConsumer(eventBufferConsumer.copy(), targetChannel);
			}

			if (flushAlways) {
				flushAll();
			}
		}
	}

	public void flushAll() {
		targetPartition.flushAll();
	}

	public void clearBuffers() {
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
	private boolean tryFinishCurrentBufferBuilder(int targetChannel, RecordSerializer<T> serializer) {

		if (!bufferBuilders[targetChannel].isPresent()) {
			return false;
		}
		BufferBuilder bufferBuilder = bufferBuilders[targetChannel].get();
		bufferBuilders[targetChannel] = Optional.empty();

		numBytesOut.inc(bufferBuilder.finish());
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

	private void closeBufferBuilder(int targetChannel) {
		if (bufferBuilders[targetChannel].isPresent()) {
			bufferBuilders[targetChannel].get().finish();
			bufferBuilders[targetChannel] = Optional.empty();
		}
	}

	public InFlightLogger getInFlightLogger() {
		return inFlightLogger;
	}

	public void checkReplayInFlightLog() throws IOException, InterruptedException {
		LOG.debug("Check for in-flight log request.");
		if (inFlightLogRequestSignalled()) {
			int channelIndex = getInFlightLogRequestSignalledChannel();
			long checkpointId = getInFlightLogRequestSignalledCheckpointId();
			int replayCounter = 0;
			LOG.debug("In-flight log request has been signalled for channel {}, checkpoint id {}.", channelIndex, checkpointId);
			Iterable<T> records = inFlightLogger.getReplayLog(channelIndex, checkpointId);

			LOG.debug("Start to replay records.");

			for (T record : records) {
				if (targetPartition instanceof ResultPartition && record instanceof SerializationDelegate) {
					LOG.debug("{}: Replay record at pos {}: {} of task {}.", inFlightLogger, replayCounter, ((SerializationDelegate) record).getInstance(), ((ResultPartition) targetPartition).getTaskName());
				} else {
					LOG.debug("{}: Replay record at pos {}: {} of partition {}.", inFlightLogger, replayCounter, record, targetPartition);
				}
				sendToTarget(record, channelIndex);
				replayCounter++;
			}

			LOG.debug("Replaying {} records completed. Reset in-flight log request flag.", replayCounter);
			resetInFlightLogRequestSignalled();
		}
	}

	private boolean inFlightLogRequestSignalled() throws IOException {
		if (targetPartition instanceof ResultPartition) {
			ResultPartition targetResultPartition = (ResultPartition) targetPartition;
			return targetResultPartition.inFlightLogRequestSignalled();
		} else {
			throw new IOException("Unable to check whether in-flight log request is received for partition of type " + targetPartition + ".");
		}
	}

	private int getInFlightLogRequestSignalledChannel() throws IOException {
		if (targetPartition instanceof ResultPartition) {
			ResultPartition targetResultPartition = (ResultPartition) targetPartition;
			return targetResultPartition.getInFlightLogRequestSignalledChannel();
		} else {
			throw new IOException("Unable to get in-flight log request signalled channel for partition of type " + targetPartition + ".");
		}
	}

	private long getInFlightLogRequestSignalledCheckpointId() throws IOException {
		if (targetPartition instanceof ResultPartition) {
			ResultPartition targetResultPartition = (ResultPartition) targetPartition;
			return targetResultPartition.getInFlightLogRequestSignalledCheckpointId();
		} else {
			throw new IOException("Unable to get in-flight log request signalled checkpoint for partition of type " + targetPartition + ".");
		}
	}

	private void resetInFlightLogRequestSignalled() throws IOException {
		if (targetPartition instanceof ResultPartition) {
			ResultPartition targetResultPartition = (ResultPartition) targetPartition;
			targetResultPartition.resetInFlightLogRequestSignalled();
		} else {
			throw new IOException("Unable to reset in-flight log request flag for partition of type " + targetPartition + ".");
		}
	}

}
