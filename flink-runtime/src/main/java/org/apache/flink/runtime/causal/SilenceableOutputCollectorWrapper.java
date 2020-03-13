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
