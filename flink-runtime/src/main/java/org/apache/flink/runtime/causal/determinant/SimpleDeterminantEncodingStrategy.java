/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.causal.determinant;


import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;

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
		if (determinant.isBufferBuiltDeterminant())
			return encodeBufferBuiltDeterminant(determinant.asBufferBuiltDeterminant());
		if (determinant.isTimerTriggerDeterminant())
			return encodeTimerTriggerDeterminant(determinant.asTimerTriggerDeterminant());
		throw new UnknownDeterminantTypeException();
	}


	//@Override
	//public List<Determinant> decode(byte[] determinants) {
	//	List<Determinant> result = new LinkedList<>();
	//	ByteBuffer b = ByteBuffer.wrap(determinants);

	//	while (b.hasRemaining()) {
	//		result.add(decodeNext(b));
	//	}
	//	return result;
	//}

	@Override
	public Determinant decodeNext(ByteBuf b) {
		if (b == null)
			return null;
		if (!b.isReadable())
			return null;
		byte tag = b.readByte();
		if (tag == Determinant.ORDER_DETERMINANT_TAG) return decodeOrderDeterminant(b);
		if (tag == Determinant.RANDOM_EMIT_DETERMINANT_TAG) return decodeRandomEmitDeterminant(b);
		if (tag == Determinant.TIMESTAMP_DETERMINANT_TAG) return decodeTimestampDeterminant(b);
		if (tag == Determinant.RNG_DETERMINANT_TAG) return decodeRNGDeterminant(b);
		if (tag == Determinant.BUFFER_BUILT_TAG) return decodeBufferBuiltDeterminant(b);
		if (tag == Determinant.TIMER_TRIGGER_DETERMINANT) return decodeTimerTriggerDeterminant(b);
		throw new CorruptDeterminantArrayException();
	}

	private Determinant decodeOrderDeterminant(ByteBuf b) {
		return new OrderDeterminant(b.readByte());
	}

	private byte[] encodeOrderDeterminant(OrderDeterminant orderDeterminant) {
		byte[] bytes = new byte[2];
		bytes[0] = Determinant.ORDER_DETERMINANT_TAG;
		bytes[1] = orderDeterminant.getChannel();
		return bytes;
	}

	private Determinant decodeTimestampDeterminant(ByteBuf b) {
		return new TimestampDeterminant(b.readLong());
	}

	private byte[] encodeTimestampDeterminant(TimestampDeterminant timestampDeterminant) {
		byte[] bytes = new byte[1 + Long.BYTES];
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.put(Determinant.TIMESTAMP_DETERMINANT_TAG);
		b.putLong(timestampDeterminant.getTimestamp());
		return b.array();
	}

	private Determinant decodeRandomEmitDeterminant(ByteBuf b) {
		return new RandomEmitDeterminant(b.readByte());
	}

	private byte[] encodeRandomEmitDeterminant(RandomEmitDeterminant randomEmitDeterminant) {
		byte[] bytes = new byte[2];
		bytes[0] = Determinant.RANDOM_EMIT_DETERMINANT_TAG;
		bytes[1] = randomEmitDeterminant.getChannel();
		return bytes;
	}

	private Determinant decodeRNGDeterminant(ByteBuf b) {
		return new RNGDeterminant(b.readInt());
	}

	private byte[] encodeRNGDeterminant(RNGDeterminant rngDeterminant) {
		byte[] bytes = new byte[1 + Integer.BYTES];
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.put(Determinant.RNG_DETERMINANT_TAG);
		b.putInt(rngDeterminant.getNumber());
		return b.array();
	}

	private Determinant decodeBufferBuiltDeterminant(ByteBuf b) {
		int bytes = b.readInt();
		return new BufferBuiltDeterminant(bytes);
	}

	private byte[] encodeBufferBuiltDeterminant(BufferBuiltDeterminant asBufferBuiltDeterminant) {
		byte[] bytes = new byte[1 + Integer.BYTES];
		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.put(Determinant.BUFFER_BUILT_TAG);
		b.putInt(asBufferBuiltDeterminant.getNumberOfBytes());
		return b.array();

	}

	private byte[] encodeTimerTriggerDeterminant(TimerTriggerDeterminant determinant) {
		ProcessingTimeCallbackID id = determinant.getProcessingTimeCallbackID();
		//1 (Tag), 4 (record count), 8 (ts), 1 (type ordinal), if internal: 4 (length of name), l (name bytes)
		byte[] bytes = new byte[1 + 4 + 8 + 1 + (id.getType() == ProcessingTimeCallbackID.Type.INTERNAL ? 4 + id.getName().getBytes().length : 0)];

		ByteBuffer b = ByteBuffer.wrap(bytes);
		b.put(Determinant.TIMER_TRIGGER_DETERMINANT);
		b.putInt(determinant.getRecordCount());
		b.putLong(determinant.getTimestamp());
		b.put((byte) id.getType().ordinal());
		if (id.getType() == ProcessingTimeCallbackID.Type.INTERNAL) {
			b.putInt(id.getName().getBytes().length);
			b.put(id.getName().getBytes());
		}

		return b.array();
	}

	private Determinant decodeTimerTriggerDeterminant(ByteBuf b) {
		int recordCount = b.readInt();
		long timestamp = b.readLong();
		ProcessingTimeCallbackID.Type type = ProcessingTimeCallbackID.Type.values()[b.readByte()];
		ProcessingTimeCallbackID id;
		if (type == ProcessingTimeCallbackID.Type.INTERNAL) {
			int numBytesOfName = b.readInt();
			byte[] nameBytes = new byte[numBytesOfName];
			b.readBytes(nameBytes);
			id = new ProcessingTimeCallbackID(new String(nameBytes));
		} else {
			id = new ProcessingTimeCallbackID(type);
		}

		return new TimerTriggerDeterminant(id, recordCount, timestamp);
	}
}
