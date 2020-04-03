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
package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.causal.CausalLoggingManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class AbstractStreamInputProcessor<IN> {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamInputProcessor.class);
	protected final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;
	protected final DeserializationDelegate<StreamElement> deserializationDelegate;
	protected final CheckpointBarrierHandler barrierHandler;
	protected final RecordWriterOutput<?>[] recordWriterOutputs;
	protected final Object lock;
	protected final CausalLoggingManager causalLoggingManager;
	/**
	 * Number of input channels the valve needs to handle.
	 */
	protected final int numInputChannels;
	protected final StreamStatusMaintainer streamStatusMaintainer;
	protected final OneInputStreamOperator<IN, ?> streamOperator;
	protected final WatermarkGauge watermarkGauge;
	protected final InputGate inputGate;

	/**
	 * Valve that controls how watermarks and stream statuses are forwarded.
	 */
	protected StatusWatermarkValve statusWatermarkValve;
	protected String taskName;
	protected RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer;
	/**
	 * The channel from which a buffer came, tracked so that we can appropriately map
	 * the watermarks and watermark statuses to channel indexes of the valve.
	 */
	protected int currentChannel = -1;
	protected Counter numRecordsIn;
	protected boolean isFinished;

	@SuppressWarnings("unchecked")
	public AbstractStreamInputProcessor(
		InputGate[] inputGates,
		TypeSerializer<IN> inputSerializer,
		StreamTask<?, ?> checkpointedTask,
		CheckpointingMode checkpointMode,
		Object lock,
		IOManager ioManager,
		Configuration taskManagerConfig,
		StreamStatusMaintainer streamStatusMaintainer,
		OneInputStreamOperator<IN, ?> streamOperator,
		TaskIOMetricGroup metrics,
		WatermarkGauge watermarkGauge,
		RecordWriterOutput<?>[] recordWriterOutputs) throws IOException {
		this.recordWriterOutputs = recordWriterOutputs;
		this.causalLoggingManager = checkpointedTask.getCausalLoggingManager();
		this.watermarkGauge = watermarkGauge;

		inputGate = InputGateUtil.createInputGate(inputGates);

		if (inputGate instanceof SingleInputGate) {
			this.taskName = ((SingleInputGate) inputGate).getTaskName();
		} else if (inputGate instanceof UnionInputGate) {
			this.taskName = ((UnionInputGate) inputGate).getTaskName();
		} else {
			this.taskName = new String("Unknown");
		}
		this.barrierHandler = InputProcessorUtil.createCheckpointBarrierHandler(
			checkpointedTask, checkpointMode, ioManager, inputGate, taskManagerConfig);

		this.lock = checkNotNull(lock);

		StreamElementSerializer<IN> ser = new StreamElementSerializer<>(inputSerializer);
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(ser);

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];

		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
				ioManager.getSpillingDirectoriesPaths());
		}

		this.numInputChannels = inputGate.getNumberOfInputChannels();

		this.streamStatusMaintainer = checkNotNull(streamStatusMaintainer);
		this.streamOperator = checkNotNull(streamOperator);

		this.statusWatermarkValve = new StatusWatermarkValve(
			numInputChannels,
			new ForwardingValveOutputHandler(streamOperator, lock));

		metrics.gauge("checkpointAlignmentTime", barrierHandler::getAlignmentDurationNanos);
	}


	public boolean processInput() throws Exception {
		if (isFinished) {
			return false;
		}
		if (numRecordsIn == null) {
			try {
				numRecordsIn = ((OperatorMetricGroup) streamOperator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
			} catch (Exception e) {
				LOG.warn("An exception occurred during the metrics setup.", e);
				numRecordsIn = new SimpleCounter();
			}
		}

		return inputLoop();
	}

	public abstract boolean inputLoop() throws Exception;

	public void cleanup() throws IOException {
		// clear the buffers first. this part should not ever fail
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			LOG.debug("cleanup: getCurrentBuffer() {}.", buffer);
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			deserializer.clear();
		}

		// cleanup the barrier handler resources
		barrierHandler.cleanup();
	}

	protected class ForwardingValveOutputHandler implements StatusWatermarkValve.ValveOutputHandler {
		private final OneInputStreamOperator<IN, ?> operator;
		private final Object lock;

		protected ForwardingValveOutputHandler(final OneInputStreamOperator<IN, ?> operator, final Object lock) {
			this.operator = checkNotNull(operator);
			this.lock = checkNotNull(lock);
		}

		@Override
		public void handleWatermark(Watermark watermark) {
			try {
				synchronized (lock) {
					watermarkGauge.setCurrentWatermark(watermark.getTimestamp());
					operator.processWatermark(watermark);
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output watermark: ", e);
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void handleStreamStatus(StreamStatus streamStatus) {
			try {
				synchronized (lock) {
					streamStatusMaintainer.toggleStreamStatus(streamStatus);
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output stream status: ", e);
			}
		}
	}
}
