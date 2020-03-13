package org.apache.flink.runtime.causal.determinant;

public class RNGDeterminant extends Determinant {
	int number;

	public RNGDeterminant(int number) {
		this.number = number;
	}

	public int getNumber() {
		return number;
	}

	public void setNumber(int number) {
		this.number = number;
	}
}
