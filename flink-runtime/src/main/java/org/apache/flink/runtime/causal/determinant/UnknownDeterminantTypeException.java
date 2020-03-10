package org.apache.flink.runtime.causal.determinant;

public class UnknownDeterminantTypeException extends RuntimeException {
	public UnknownDeterminantTypeException() {
		super("Encoding strategy does not know how to encode this determinant");
	}
}
