package org.apache.flink.runtime.causal.determinant;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public abstract class AbstractDeterminant implements Determinant, IOReadableWritable {

}
