package org.apache.flink.runtime.causal.determinant;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.causal.determinant.AbstractDeterminant;

import java.io.IOException;

public class RNGDeterminant extends AbstractDeterminant {

	private int integer;

	public RNGDeterminant(){
	}

	public int getInteger(){return integer;}

	public void setInteger(int integer){
		this.integer = integer;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(integer);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.setInteger(in.readInt());
	}

}
