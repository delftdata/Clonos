package org.apache.flink.runtime.causal.determinant;

public abstract class Determinant {

	public static final byte ORDER_DETERMINANT_TAG = 0;


	public boolean isOrderDeterminant() {
		return getClass() == OrderDeterminant.class;
	}

	public OrderDeterminant asOrderDeterminant() {
		return (OrderDeterminant) this;
	}

}
