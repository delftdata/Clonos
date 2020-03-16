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
package org.apache.flink.runtime.causal;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.util.OutputTag;

public class SilenceableOutputCollectorWrapper<T> implements OperatorChain.WatermarkGaugeExposingOutput<StreamRecord<T>>, Silenceable {

	private OperatorChain.WatermarkGaugeExposingOutput<StreamRecord<T>> wrapped;
	private boolean silenced;

	public SilenceableOutputCollectorWrapper(OperatorChain.WatermarkGaugeExposingOutput<StreamRecord<T>> output) {
		this.wrapped = output;
	}

	@Override
	public void silence() {
		this.silenced = true;
	}

	@Override
	public void unsilence() {
		this.silenced = false;
	}

	@Override
	public Gauge<Long> getWatermarkGauge() {
		return wrapped.getWatermarkGauge();
	}

	@Override
	public void emitWatermark(Watermark watermark) {
		if (!silenced)
			wrapped.emitWatermark(watermark);
	}

	@Override
	public void emitLatencyMarker(LatencyMarker latencyMarker) {
		if (!silenced)
			wrapped.emitLatencyMarker(latencyMarker);
	}

	@Override
	public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> streamRecord) {
		if (!silenced)
			wrapped.collect(outputTag, streamRecord);

	}

	@Override
	public void collect(StreamRecord<T> record) {
		if (!silenced)
			wrapped.collect(record);
	}

	@Override
	public void close() {
		wrapped.close();
	}
}
