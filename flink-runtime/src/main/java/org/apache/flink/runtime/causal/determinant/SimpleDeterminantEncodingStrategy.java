package org.apache.flink.runtime.causal.determinant;


import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class SimpleDeterminantEncodingStrategy implements DeterminantEncodingStrategy {
	/*
	According to google object pooling is not worth it anymore. Possible todo is to use a tiered pool of byte array lengths
	 */
	@Override
	public byte[] encode(Determinant determinant) {
		//2 bytes
		if (determinant.isOrderDeterminant()) return encodeOrderDeterminant(determinant.asOrderDeterminant());
		throw new UnknownDeterminantTypeException();
	}

	@Override
	public List<Determinant> decode(byte[] determinants) {
		List<Determinant> result = new LinkedList<>();
		ByteBuffer b = ByteBuffer.wrap(determinants);

		while (b.hasRemaining()) {
			byte tag = b.get();
			if (tag == Determinant.ORDER_DETERMINANT_TAG) result.add(decodeOrderDeterminant(b));
			throw new CorruptDeterminantArrayException();
		}
		return result;
	}

	private Determinant decodeOrderDeterminant(ByteBuffer b) {
		return new OrderDeterminant((int) b.get());
	}

	private byte[] encodeOrderDeterminant(OrderDeterminant orderDeterminant) {
		byte[] bytes = new byte[2];
		bytes[0] = Determinant.ORDER_DETERMINANT_TAG;
		bytes[1] = (byte) orderDeterminant.getChannel();
		return bytes;
	}
}
