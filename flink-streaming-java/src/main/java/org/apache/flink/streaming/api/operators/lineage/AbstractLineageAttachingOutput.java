package org.apache.flink.streaming.api.operators.lineage;

import org.apache.curator.shaded.com.google.common.hash.HashFunction;
import org.apache.curator.shaded.com.google.common.hash.Hashing;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

public abstract class AbstractLineageAttachingOutput<OUT> implements LineageAttachingOutput<OUT> {

	protected Output<StreamRecord<OUT>> outputToWrap;

	protected HashFunction hashFunction;

	protected static final String LINEAGE_INFO_NAME = "__LINEAGE_INFO__";

	public AbstractLineageAttachingOutput(Output<StreamRecord<OUT>> outputToWrap) {
		this.outputToWrap = outputToWrap;
		this.hashFunction = Hashing.goodFastHash(RecordID.NUMBER_OF_BYTES * 8);
	}

	@Override
	public abstract void notifyInputRecord(StreamRecord<?> input);

	@Override
	public abstract void initializeState(StateInitializationContext context) throws Exception;

	@Override
	public abstract void snapshotState(StateSnapshotContext context) throws Exception;

	protected abstract RecordID getRecordIDForNextOutputRecord();

	@Override
	public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
		record.setRecordID(getRecordIDForNextOutputRecord());
		outputToWrap.collect(outputTag, record);
	}


	@Override
	public void collect(StreamRecord<OUT> record) {
		record.setRecordID(getRecordIDForNextOutputRecord());
		outputToWrap.collect(record);
	}

	@Override
	public void emitWatermark(Watermark mark) {
		outputToWrap.emitWatermark(mark);
	}

	@Override
	public void emitLatencyMarker(LatencyMarker latencyMarker) {
		outputToWrap.emitLatencyMarker(latencyMarker);
	}

	@Override
	public void close() {
		outputToWrap.close();
	}
}
