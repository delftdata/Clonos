package org.apache.flink.runtime.causal.determinant;

public class OrderDeterminant extends Determinant {

	private int channel;

	public OrderDeterminant(int channel) {
		this.channel = channel;
	}

	public int getChannel() {
		return channel;
	}

	public void setChannel(int channel) {
		this.channel = channel;
	}
}
