package org.apache.flink.runtime.causal.determinant;

public class CorruptDeterminantArrayException extends RuntimeException {

	public CorruptDeterminantArrayException() {
		super("Error while decoding determinants");
	}

}
