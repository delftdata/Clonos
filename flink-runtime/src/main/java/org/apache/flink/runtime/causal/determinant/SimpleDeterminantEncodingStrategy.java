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
		if (determinant.isOrderDeterminant()) return encodeOrderDeterminant(determinant.asOrderDeterminant());
		if (determinant.isRandomEmitDeterminant())
			return encodeRandomEmitDeterminant(determinant.asRandomEmitDeterminant());
		if (determinant.isTimestampDeterminant())
			return encodeTimestampDeterminant(determinant.asTimestampDeterminant());
		if (determinant.isRNGDeterminant()) return encodeRNGDeterminant(determinant.asRNGDeterminant());
		throw new UnknownDeterminantTypeException();
	}

	@Override
	public List<Determinant> decode(byte[] determinants) {
		List<Determinant> result = new LinkedList<>();
		ByteBuffer b = ByteBuffer.wrap(determinants);

		while (b.hasRemaining()) {
			byte tag = b.get();
			if (tag == Determinant.ORDER_DETERMINANT_TAG) result.add(decodeOrderDeterminant(b));
			if (tag == Determinant.RANDOMEMIT_DETERMINANT_TAG) result.add(decodeRandomEmitDeterminant(b));
			if (tag == Determinant.TIMESTAMP_DETERMINANT_TAG) result.add(decodeTimestampDeterminant(b));
			if (tag == Determinant.RNG_DETERMINANT_TAG) result.add(decodeRNGDeterminant(b));
			throw new CorruptDeterminantArrayException();
		}
		return result;
	}

	private Determinant decodeOrderDeterminant(ByteBuffer b) {
		return new OrderDeterminant(b.get());
	}

	private byte[] encodeOrderDeterminant(OrderDeterminant orderDeterminant) {
		byte[] bytes = new byte[2];
		//bytes[0] = Determinant.ORDER_DETERMINANT_TAG;
		bytes[1] = orderDeterminant.getChannel();
		return bytes;
	}

	private Determinant decodeTimestampDeterminant(ByteBuffer b) {
		return new TimestampDeterminant(b.getLong());
	}

	private byte[] encodeTimestampDeterminant(TimestampDeterminant timestampDeterminant) {
		byte[] bytes = new byte[1 + Long.BYTES];
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.put(Determinant.TIMESTAMP_DETERMINANT_TAG);
		b.putLong(timestampDeterminant.getTimestamp());
		return b.array();
	}

	private Determinant decodeRandomEmitDeterminant(ByteBuffer b) {
		return new RandomEmitDeterminant(b.get());
	}

	private byte[] encodeRandomEmitDeterminant(RandomEmitDeterminant randomEmitDeterminant) {
		byte[] bytes = new byte[2];
		bytes[0] = Determinant.RANDOMEMIT_DETERMINANT_TAG;
		bytes[1] = randomEmitDeterminant.getChannel();
		return bytes;
	}

	private Determinant decodeRNGDeterminant(ByteBuffer b) {
		return new RNGDeterminant(b.getInt());
	}

	private byte[] encodeRNGDeterminant(RNGDeterminant rngDeterminant) {
		byte[] bytes = new byte[1 + Integer.BYTES];
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.put(Determinant.RNG_DETERMINANT_TAG);
		b.putInt(rngDeterminant.getNumber());
		return b.array();
	}
}
