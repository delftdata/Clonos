package org.apache.flink.streaming.api.operators.lineage;

public interface KeyedLineageAttachingOutput<K, OUT> extends LineageAttachingOutput<OUT> {

	void setKey(K key);

}
