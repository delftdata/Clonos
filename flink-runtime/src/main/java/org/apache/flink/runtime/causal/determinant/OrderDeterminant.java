package org.apache.flink.runtime.causal.determinant;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class OrderDeterminant extends AbstractDeterminant{

	//Represents the ID of the next message to be delivered
	private byte operatorId;

	public OrderDeterminant() {
	}

	public byte getOperatorId() {
		return operatorId;
	}

	public void setOperatorId(byte operatorId) {
		this.operatorId = operatorId;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.write(operatorId);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.setOperatorId(in.readByte());
	}
}
