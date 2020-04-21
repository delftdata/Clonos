package org.apache.flink.runtime.inflightlogging;

import java.util.ListIterator;

public interface SizedListIterator<T> extends ListIterator<T> {

	int numberRemaining();

}
