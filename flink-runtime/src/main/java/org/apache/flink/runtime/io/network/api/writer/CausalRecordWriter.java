/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.causal.ICausalLoggingManager;
import org.apache.flink.runtime.causal.DeterminantCarrier;
import org.apache.flink.runtime.causal.services.RandomService;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CausalRecordWriter<T extends IOReadableWritable> extends RecordWriter<T> {

	private ICausalLoggingManager causalLoggingManager;


	private static final Logger LOG = LoggerFactory.getLogger(CausalRecordWriter.class);

	private RandomService randomService;

	public CausalRecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector, boolean flushAlways, ICausalLoggingManager causalLoggingManager, RandomService randomService) {
		super(writer, channelSelector, flushAlways);
		this.causalLoggingManager = causalLoggingManager;
		this.randomService = randomService;
	}

	@Override
	public void randomEmit(T record) throws IOException, InterruptedException {
		sendToTarget(record, randomService.nextInt(numChannels));
	}

	@Override
	protected void sendToTarget(T record, int targetChannel) throws IOException, InterruptedException {
		//Essentially the same operation, except if silenced we do not send it out. Additionally, we enrich it with deltas.
		//Todo this is a really bad cast
		causalLoggingManager.enrichWithDeltas((DeterminantCarrier) record, targetChannel);
		super.sendToTarget(record, targetChannel);
	}

	@Override
	public void broadcastEvent(AbstractEvent event) throws IOException, InterruptedException {
		LOG.debug("{}: RecordWriter broadcast event {}.", targetPartition.getTaskName(), event);
		for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
			if (event instanceof CheckpointBarrier) {
				CheckpointBarrier toCopy = (CheckpointBarrier) event;
				CheckpointBarrier copy = new CheckpointBarrier(toCopy.getId(), toCopy.getTimestamp(), toCopy.getCheckpointOptions());
				causalLoggingManager.enrichWithDeltas(copy, targetChannel);
				emitEvent(copy, targetChannel);
			} else {
				emitEvent(event, targetChannel);
			}
		}
	}

	@Override
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
}
