package org.apache.flink.runtime.causal.determinant;

public abstract class Determinant {

	public static final byte ORDER_DETERMINANT_TAG = 0;
	public static final byte TIMESTAMP_DETERMINANT_TAG = 1;
	public static final byte RANDOMEMIT_DETERMINANT_TAG = 2;
	public static final byte RNG_DETERMINANT_TAG = 3;


	public boolean isOrderDeterminant() {
		return getClass() == OrderDeterminant.class;
	}

	public OrderDeterminant asOrderDeterminant() {
		return (OrderDeterminant) this;
	}

	public boolean isTimestampDeterminant() {
		return getClass() == TimestampDeterminant.class;
	}

	public TimestampDeterminant asTimestampDeterminant() {
		return (TimestampDeterminant) this;
	}

	public boolean isRandomEmitDeterminant() {
		return getClass() == RandomEmitDeterminant.class;
	}

	public RandomEmitDeterminant asRandomEmitDeterminant() {
		return (RandomEmitDeterminant) this;
	}

	public boolean isRNGDeterminant() {
		return getClass() == RNGDeterminant.class;
	}

	public RNGDeterminant asRNGDeterminant() {
		return (RNGDeterminant) this;
	}
}
