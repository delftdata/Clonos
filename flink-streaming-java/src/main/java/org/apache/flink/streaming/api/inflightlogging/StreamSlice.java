package org.apache.flink.streaming.api.inflightlogging;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;

public class StreamSlice<OUT> {

	String sliceId;
	List<StreamRecord<OUT>> sliceRecords;

	public StreamSlice(String sliceId) {
		this.sliceId = sliceId;
		this.sliceRecords = new ArrayList<>(); // TODO check what is a sane initial size
	}

	public StreamSlice(String sliceId, List<StreamRecord<OUT>> sliceRecords) {
		this.sliceId = sliceId;
		this.sliceRecords = sliceRecords;
	}

	public String getSliceId() {
		return sliceId;
	}

	public void setSliceId(String sliceId) {
		this.sliceId = sliceId;
	}

	public List<StreamRecord<OUT>> getSliceRecords() {
		return sliceRecords;
	}

	public void setSliceRecords(List<StreamRecord<OUT>> sliceRecords) {
		this.sliceRecords = sliceRecords;
	}

	public void addRecord(StreamRecord<OUT> newRecord) {
		this.sliceRecords.add(newRecord);
	}
}
