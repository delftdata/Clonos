package org.apache.flink.streaming.api.operators.lineage;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.RecordID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractWindowLineageAttachingOutput<K, W, OUT> extends AbstractLineageAttachingOutput<OUT> implements WindowLineageAttachingOutput<K, W, OUT> {


	/**
	 * Maps a pane identifier (Stream Key + Window) to an under construction RecordID and a counter for how many times it has been output.
	 */
	protected ListState<Tuple2<Tuple2<K, W>, Tuple2<RecordID, Integer>>> paneToRecordIDReductionManaged;

	protected Map<Tuple2<K, W>, Tuple2<RecordID, Integer>> paneToRecordIDReduction;

	protected RecordID latestInserted;

	protected Tuple2<K, W> currentContext;

	public AbstractWindowLineageAttachingOutput(Output<StreamRecord<OUT>> outputToWrap) {
		super(outputToWrap);
		paneToRecordIDReduction = new HashMap<>();
		this.currentContext = new Tuple2<>();
	}


	@Override
	public void setCurrentKey(K key) {
		this.currentContext.f0 = key;
	}

	@Override
	public void notifyAssignedWindows(Collection<W> windows) {
		for (W window : windows) {
			Tuple2<RecordID, Integer> current = paneToRecordIDReduction.get(currentContext);
			if (current != null) {
				RecordID.mergeIntoFirst(current.f0, this.latestInserted);
				current.f1 = 0; // Pane has been updated. Reset counter of outputed with current state.
			} else
				paneToRecordIDReduction.put(currentContext.copy(), new Tuple2<>(this.latestInserted, 0)); //todo check if need clone here
		}
	}

	@Override
	public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
		//Side outputs are for late elements. Dont attach lineage.
		this.outputToWrap.collect(outputTag, record);
	}

	@Override
	public void setCurrentWindow(W window) {
		this.currentContext.f1 = window;
	}

	@Override
	public void notifyPurgedWindow() {
		this.paneToRecordIDReduction.remove(currentContext);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		this.paneToRecordIDReductionManaged = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(LINEAGE_INFO_NAME, TypeInformation.of(new TypeHint<Tuple2<Tuple2<K, W>, Tuple2<RecordID, Integer>>>() {
		})));
		if (context.isRestored())
			for (Tuple2<Tuple2<K, W>, Tuple2<RecordID, Integer>> mapping : paneToRecordIDReductionManaged.get())
				this.paneToRecordIDReduction.put(mapping.f0, mapping.f1);
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		this.paneToRecordIDReductionManaged.clear();
		for (Map.Entry<Tuple2<K, W>, Tuple2<RecordID, Integer>> mapping : paneToRecordIDReduction.entrySet())
			this.paneToRecordIDReductionManaged.add(new Tuple2<>(mapping.getKey(), mapping.getValue()));
	}

}
