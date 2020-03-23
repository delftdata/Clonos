package org.apache.flink.streaming.api.operators.lineage;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.RecordID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * @param <K>
 * @param <W>
 * @param <OUT>
 */
public class NonMergingWindowLineageAttachingOutput<K, W, OUT> extends AbstractWindowLineageAttachingOutput<K, W, OUT> {


	public NonMergingWindowLineageAttachingOutput(Output<StreamRecord<OUT>> outputToWrap) {
		super(outputToWrap);
	}


	@Override
	public void notifyInputRecord(StreamRecord<?> input) {
		latestInserted = input.getRecordID();
	}


	@Override
	protected RecordID getRecordIDForNextOutputRecord() {
		Tuple2<RecordID, Integer> current = this.paneToRecordIDReduction.get(currentContext);
		RecordID toReturn = new RecordID();
		hashFunction.hashInt(current.f0.hashCode() + current.f1).writeBytesTo(toReturn.getId(), 0, RecordID.NUMBER_OF_BYTES);
		current.f1++; //Increase the counter.
		return toReturn;
	}

}
