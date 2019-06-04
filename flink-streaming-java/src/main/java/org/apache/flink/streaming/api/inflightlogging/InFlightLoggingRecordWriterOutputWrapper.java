package org.apache.flink.streaming.api.inflightlogging;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.OperatorChainOutput;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusProvider;
import org.apache.flink.util.OutputTag;

import java.io.IOException;

public class InFlightLoggingRecordWriterOutputWrapper<OUT> implements OperatorChainOutput<OUT> {
	private OperatorChainOutput<OUT> recordWriterOut;
	private int channelIndex;
	private InFlightLogger<OUT> inFlightLogger;


	public InFlightLoggingRecordWriterOutputWrapper(OperatorChainOutput<OUT> recordWriterOut, int channelIndex, InFlightLogger<OUT> inFlightLogger) {
		this.recordWriterOut = recordWriterOut;
		this.channelIndex = channelIndex;
		this.inFlightLogger = inFlightLogger;
	}

	@Override
	public void emitWatermark(Watermark mark) {
		recordWriterOut.emitWatermark(mark);
	}

	@Override
	public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
		recordWriterOut.collect(outputTag, record);
		inFlightLogger.logRecord((StreamRecord<OUT>)record, this.channelIndex);
	}

	@Override
	public void emitLatencyMarker(LatencyMarker latencyMarker) {
		recordWriterOut.emitLatencyMarker(latencyMarker);
	}

	@Override
	public void collect(StreamRecord<OUT> record) {
		recordWriterOut.collect(record);
		inFlightLogger.logRecord(record, this.channelIndex);
	}

	@Override
	public void close() {
		recordWriterOut.close();
	}

	@Override
	public void emitStreamStatus(StreamStatus streamStatus) {
		recordWriterOut.emitStreamStatus(streamStatus);
	}

	@Override
	public void broadcastEvent(AbstractEvent event) throws IOException {
		recordWriterOut.broadcastEvent(event);
	}

	@Override
	public void flush() throws IOException {
		recordWriterOut.flush();
	}

	@Override
	public Gauge<Long> getWatermarkGauge() {
		return  recordWriterOut.getWatermarkGauge();
	}
}
