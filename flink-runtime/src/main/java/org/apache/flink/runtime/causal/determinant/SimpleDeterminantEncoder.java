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


import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;

import java.util.Queue;

public class SimpleDeterminantEncoder implements DeterminantEncoder {

	@Override
	public byte[] encode(Determinant determinant) {
		if (determinant.isOrderDeterminant())
			return encodeOrderDeterminant(determinant.asOrderDeterminant());
		if (determinant.isTimestampDeterminant())
			return encodeTimestampDeterminant(determinant.asTimestampDeterminant());
		if (determinant.isRNGDeterminant())
			return encodeRNGDeterminant(determinant.asRNGDeterminant());
		if (determinant.isBufferBuiltDeterminant())
			return encodeBufferBuiltDeterminant(determinant.asBufferBuiltDeterminant());
		if (determinant.isTimerTriggerDeterminant())
			return encodeTimerTriggerDeterminant(determinant.asTimerTriggerDeterminant());
		if (determinant.isSourceCheckpointDeterminant())
			return encodeSourceCheckpointDeterminant(determinant.asSourceCheckpointDeterminant());
		throw new UnknownDeterminantTypeException();
	}

	@Override
	public void encodeTo(Determinant determinant, ByteBuf targetBuf) {
		if (determinant.isOrderDeterminant())
			encodeOrderDeterminant(determinant.asOrderDeterminant(), targetBuf);
		if (determinant.isTimestampDeterminant())
			encodeTimestampDeterminant(determinant.asTimestampDeterminant(), targetBuf);
		if (determinant.isRNGDeterminant())
			encodeRNGDeterminant(determinant.asRNGDeterminant(), targetBuf);
		if (determinant.isBufferBuiltDeterminant())
			encodeBufferBuiltDeterminant(determinant.asBufferBuiltDeterminant(), targetBuf);
		if (determinant.isTimerTriggerDeterminant())
			encodeTimerTriggerDeterminant(determinant.asTimerTriggerDeterminant(), targetBuf);
		if (determinant.isSourceCheckpointDeterminant())
			encodeSourceCheckpointDeterminant(determinant.asSourceCheckpointDeterminant(), targetBuf);
	}

	@Override
	public Determinant decodeNext(ByteBuf b) {
		if (b == null)
			return null;
		if (!b.isReadable())
			return null;
		byte tag = b.readByte();
		if (tag == Determinant.ORDER_DETERMINANT_TAG) return decodeOrderDeterminant(b);
		if (tag == Determinant.TIMESTAMP_DETERMINANT_TAG) return decodeTimestampDeterminant(b);
		if (tag == Determinant.RNG_DETERMINANT_TAG) return decodeRNGDeterminant(b);
		if (tag == Determinant.BUFFER_BUILT_TAG) return decodeBufferBuiltDeterminant(b);
		if (tag == Determinant.TIMER_TRIGGER_DETERMINANT) return decodeTimerTriggerDeterminant(b);
		if (tag == Determinant.SOURCE_CHECKPOINT_DETERMINANT) return decodeSourceCheckpointDeterminant(b);
		throw new CorruptDeterminantArrayException();
	}

	@Override
	public Determinant decodeNext(ByteBuf b, Queue<Determinant>[] determinantCache) {
		if (b == null)
			return null;
		if (!b.isReadable())
			return null;
		byte tag = b.readByte();
		if (tag == Determinant.ORDER_DETERMINANT_TAG) return decodeOrderDeterminant(b, (OrderDeterminant) determinantCache[tag].poll());
		if (tag == Determinant.TIMESTAMP_DETERMINANT_TAG) return decodeTimestampDeterminant(b, (TimestampDeterminant) determinantCache[tag].poll());
		if (tag == Determinant.RNG_DETERMINANT_TAG) return decodeRNGDeterminant(b, (RNGDeterminant) determinantCache[tag].poll());
		if (tag == Determinant.BUFFER_BUILT_TAG) return decodeBufferBuiltDeterminant(b, (BufferBuiltDeterminant) determinantCache[tag].poll());
		if (tag == Determinant.TIMER_TRIGGER_DETERMINANT) return decodeTimerTriggerDeterminant(b, (TimerTriggerDeterminant) determinantCache[tag].poll());
		if (tag == Determinant.SOURCE_CHECKPOINT_DETERMINANT) return decodeSourceCheckpointDeterminant(b, (SourceCheckpointDeterminant) determinantCache[tag].poll());
		throw new CorruptDeterminantArrayException();
	}

	@Override
	public Determinant decodeOrderDeterminant(ByteBuf b) {
		return decodeOrderDeterminant(b, new OrderDeterminant());
	}
	@Override
	public Determinant decodeOrderDeterminant(ByteBuf b, OrderDeterminant reuse) {
		return reuse.replace(b.readByte());
	}

	private void encodeOrderDeterminant(OrderDeterminant orderDeterminant, ByteBuf buf) {
		buf.writeByte(Determinant.ORDER_DETERMINANT_TAG);
		buf.writeByte(orderDeterminant.getChannel());
	}

	private byte[] encodeOrderDeterminant(OrderDeterminant orderDeterminant) {
		byte[] bytes = new byte[orderDeterminant.getEncodedSizeInBytes()];
		ByteBuf buf = Unpooled.wrappedBuffer(bytes);
		encodeOrderDeterminant(orderDeterminant, buf);
		return bytes;
	}

	@Override
	public Determinant decodeTimestampDeterminant(ByteBuf b) {
		return decodeTimestampDeterminant(b, new TimestampDeterminant());
	}

	@Override
	public Determinant decodeTimestampDeterminant(ByteBuf b, TimestampDeterminant reuse) {
		return reuse.replace(b.readLong());
	}
	private void encodeTimestampDeterminant(TimestampDeterminant timestampDeterminant, ByteBuf buf) {
		buf.writeByte(Determinant.TIMESTAMP_DETERMINANT_TAG);
		buf.writeLong(timestampDeterminant.getTimestamp());
	}

	private byte[] encodeTimestampDeterminant(TimestampDeterminant timestampDeterminant) {
		byte[] bytes = new byte[timestampDeterminant.getEncodedSizeInBytes()];
		ByteBuf buf = Unpooled.wrappedBuffer(bytes);
		encodeTimestampDeterminant(timestampDeterminant, buf);
		return bytes;
	}

	@Override
	public Determinant decodeRNGDeterminant(ByteBuf b) {
		return decodeRNGDeterminant(b, new RNGDeterminant());
	}

	@Override
	public Determinant decodeRNGDeterminant(ByteBuf b, RNGDeterminant reuse) {
		return reuse.replace(b.readInt());
	}

	private void encodeRNGDeterminant(RNGDeterminant rngDeterminant, ByteBuf buf) {
		buf.writeByte(Determinant.RNG_DETERMINANT_TAG);
		buf.writeInt(rngDeterminant.getNumber());
	}

	private byte[] encodeRNGDeterminant(RNGDeterminant rngDeterminant) {
		byte[] bytes = new byte[rngDeterminant.getEncodedSizeInBytes()];
		ByteBuf buf = Unpooled.wrappedBuffer(bytes);
		encodeRNGDeterminant(rngDeterminant, buf);
		return bytes;
	}

	@Override
	public Determinant decodeBufferBuiltDeterminant(ByteBuf b) {
		return decodeBufferBuiltDeterminant(b, new BufferBuiltDeterminant());
	}

	@Override
	public Determinant decodeBufferBuiltDeterminant(ByteBuf b, BufferBuiltDeterminant reuse) {
		return reuse.replace(b.readInt());
	}

	private void encodeBufferBuiltDeterminant(BufferBuiltDeterminant bufferBuiltDeterminant, ByteBuf buf) {
		buf.writeByte(Determinant.BUFFER_BUILT_TAG);
		buf.writeInt(bufferBuiltDeterminant.getNumberOfBytes());
	}

	private byte[] encodeBufferBuiltDeterminant(BufferBuiltDeterminant bufferBuiltDeterminant) {
		byte[] bytes = new byte[bufferBuiltDeterminant.getEncodedSizeInBytes()];
		ByteBuf buf = Unpooled.wrappedBuffer(bytes);
		encodeBufferBuiltDeterminant(bufferBuiltDeterminant, buf);
		return bytes;

	}

	private void encodeTimerTriggerDeterminant(TimerTriggerDeterminant determinant, ByteBuf buf) {
		ProcessingTimeCallbackID id = determinant.getProcessingTimeCallbackID();

		buf.writeByte(Determinant.TIMER_TRIGGER_DETERMINANT);
		buf.writeInt(determinant.getRecordCount());
		buf.writeLong(determinant.getTimestamp());
		buf.writeByte((byte) id.getType().ordinal());
		if (id.getType() == ProcessingTimeCallbackID.Type.INTERNAL) {
			buf.writeInt(id.getName().getBytes().length);
			buf.writeBytes(id.getName().getBytes());
		}
	}

	private byte[] encodeTimerTriggerDeterminant(TimerTriggerDeterminant determinant) {
		byte[] bytes = new byte[determinant.getEncodedSizeInBytes()];
		ByteBuf buf = Unpooled.wrappedBuffer(bytes);
		encodeTimerTriggerDeterminant(determinant, buf);
		return bytes;
	}

	@Override
	public Determinant decodeTimerTriggerDeterminant(ByteBuf b) {
		return decodeTimerTriggerDeterminant(b, new TimerTriggerDeterminant());
	}

	@Override
	public Determinant decodeTimerTriggerDeterminant(ByteBuf b, TimerTriggerDeterminant reuse){
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
		return reuse.replace(recordCount, id, timestamp);
	}

	private void encodeSourceCheckpointDeterminant(SourceCheckpointDeterminant det, ByteBuf buf) {
		byte[] ref = det.getStorageReference();

		buf.writeByte(Determinant.SOURCE_CHECKPOINT_DETERMINANT);
		buf.writeInt(det.getRecordCount());
		buf.writeLong(det.getCheckpointID());
		buf.writeLong(det.getCheckpointTimestamp());
		buf.writeByte((byte) det.getType().ordinal());
		buf.writeByte((byte) (ref == null ? 0 : 1));
		if (ref != null) {
			buf.writeInt(ref.length);
			buf.writeBytes(ref);
		}
	}

	private byte[] encodeSourceCheckpointDeterminant(SourceCheckpointDeterminant det) {
		// tag (1), rec count (4), checkpoint (8), ts (8), type ordinal (1), has ref? (1), ref length (4), ref (X)
		byte[] bytes = new byte[det.getEncodedSizeInBytes()];
		ByteBuf buf = Unpooled.wrappedBuffer(bytes);
		encodeSourceCheckpointDeterminant(det, buf);
		return bytes;
	}

	@Override
	public Determinant decodeSourceCheckpointDeterminant(ByteBuf b) {
		return decodeSourceCheckpointDeterminant(b, new SourceCheckpointDeterminant());
	}

	@Override
	public Determinant decodeSourceCheckpointDeterminant(ByteBuf b, SourceCheckpointDeterminant reuse) {
		int recCount = b.readInt();
		long checkpoint = b.readLong();
		long ts = b.readLong();
		byte typeOrd = b.readByte();
		boolean hasRef = b.readBoolean();
		byte[] ref = null;
		if (hasRef) {
			int length = b.readInt();
			ref = new byte[length];
			b.readBytes(ref);
		}
		return reuse.replace(recCount, checkpoint, ts, CheckpointType.values()[typeOrd], ref);

	}
}
