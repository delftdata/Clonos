package org.apache.flink.runtime.causal.determinant;

public class RandomEmitDeterminant extends Determinant {
	byte channel;

	public RandomEmitDeterminant(byte channel) {
		this.channel = channel;
	}

	public byte getChannel() {
		return channel;
	}

	public void setChannel(byte channel) {
		this.channel = channel;
	}
}
