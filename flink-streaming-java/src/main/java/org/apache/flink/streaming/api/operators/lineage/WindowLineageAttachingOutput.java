package org.apache.flink.streaming.api.operators.lineage;

import java.util.Collection;

public interface WindowLineageAttachingOutput<K, W, OUT> extends LineageAttachingOutput<OUT> {


	void setCurrentKey(K key);


	void notifyAssignedWindows(Collection<W> windows);

	void setCurrentWindow(W window);

	void notifyPurgedWindow();
}
