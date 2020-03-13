package org.apache.flink.runtime.causal.determinant;

public class TimestampDeterminant extends Determinant {
	long timestamp;


	public TimestampDeterminant(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
}
