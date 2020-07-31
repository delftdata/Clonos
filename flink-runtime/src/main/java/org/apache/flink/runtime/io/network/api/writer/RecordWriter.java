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
import org.apache.flink.runtime.causal.CheckpointBarrierListener;
import org.apache.flink.runtime.causal.services.CausalRandomService;
import org.apache.flink.runtime.causal.services.RandomService;
import org.apache.flink.runtime.causal.services.SimpleRandomService;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.state.CheckpointListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

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
public class RecordWriter<T extends IOReadableWritable> implements CheckpointListener, CheckpointBarrierListener {

	private static final Logger LOG = LoggerFactory.getLogger(RecordWriter.class);

	protected final ResultPartitionWriter targetPartition;

	protected final ChannelSelector<T> channelSelector;

	protected final int numChannels;

	/**
	 * {@link RecordSerializer} per outgoing channel.
	 */
	protected final RecordSerializer<T>[] serializers;

	protected final Optional<BufferBuilder>[] bufferBuilders;


	protected final boolean flushAlways;

	protected Counter numBytesOut = new SimpleCounter();

	protected RandomService randomService;

	public RecordWriter(ResultPartitionWriter writer) {
		this(writer, new RoundRobinChannelSelector<T>());
	}

	@SuppressWarnings("unchecked")
	public RecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector) {
		this(writer, channelSelector, false, new SimpleRandomService());
	}

	public RecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector, boolean flushAlways, RandomService randomService) {
		this.flushAlways = flushAlways;
		this.targetPartition = writer;
		this.channelSelector = channelSelector;
		this.channelSelector.setRandomService(randomService);
		this.randomService = randomService;

		this.numChannels = writer.getNumberOfSubpartitions();

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

	public ResultPartitionWriter getResultPartition() {
		return targetPartition;
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
		sendToTarget(record, randomService.nextInt(numChannels));
	}

	protected void sendToTarget(T record, int targetChannel) throws IOException, InterruptedException {
		RecordSerializer<T> serializer = serializers[targetChannel];
		LOG.debug("RecordWriter send to target");

		SerializationResult result = serializer.addRecord(record);


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
		LOG.info("{}: RecordWriter broadcast event {}.", targetPartition.getTaskName(), event);

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


	public void emitEvent(AbstractEvent event, int targetChannel) throws IOException, InterruptedException {
		LOG.info("{}: RecordWriter emit event {}.", targetPartition.getTaskName(), event);

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
		}
	}

	@Override
	public void notifyCheckpointBarrier(long checkpointID) {
		targetPartition.notifyCheckpointBarrier(checkpointID);
		channelSelector.notifyCheckpointBarrier(checkpointID);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		targetPartition.notifyCheckpointComplete(checkpointId);
	}
}
