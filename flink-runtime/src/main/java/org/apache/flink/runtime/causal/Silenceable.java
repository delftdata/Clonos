package org.apache.flink.runtime.causal;

public interface Silenceable {

	public void silence();

	public void unsilence();
}
