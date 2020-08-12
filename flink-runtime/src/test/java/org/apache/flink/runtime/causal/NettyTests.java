/*
 *
 *
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 *
 *
 */

package org.apache.flink.runtime.causal;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.junit.Test;

public class NettyTests {

	@Test
	public void CompositeByteBufferTest() {

		ByteBuf b1 = Unpooled.buffer();

		b1.writeBytes("Hello ".getBytes());


		ByteBuf b2 = Unpooled.buffer();
		b2.writeBytes("world".getBytes());

		CompositeByteBuf cb = Unpooled.compositeBuffer();
		cb.addComponents(true, b1, b2);

		byte[] bytes = new byte[cb.readableBytes()];
		cb.readBytes(bytes);
		assert b1.readerIndex() == 0;
		assert new String(bytes).equals("Hello world");

		CompositeByteBuf cb2 = Unpooled.compositeBuffer();
		cb2.addComponents(true, b1, b2);
		byte[] bytes2 = new byte[cb2.readableBytes()];
		cb.readBytes(bytes2);
		assert new String(bytes2).equals("Hello world");
	}

	@Test
	public void CompositeByteBufferReleaseTest() {

		ByteBuf b1 = Unpooled.buffer();

		b1.writeBytes("Hello ".getBytes());


		ByteBuf b2 = Unpooled.buffer();
		b2.writeBytes("world".getBytes());

		CompositeByteBuf cb = Unpooled.compositeBuffer();
		cb.addComponents(true, b1.retain(), b2.retain());

		assert b1.refCnt() == 2;
		assert cb.refCnt() == 1;

		byte[] read1 = new byte[cb.readableBytes()];
		cb.readBytes(read1);
		assert new String(read1).equals("Hello world");

		assert cb.release();
		assert b1.refCnt() == 1;
		assert cb.refCnt() == 0;

		CompositeByteBuf cb2 = Unpooled.compositeBuffer();
		cb2.addComponents(true, b1.retain(), b2.retain());

		assert b1.refCnt() == 2;
		assert cb2.refCnt() == 1;

		byte[] read2 = new byte[cb2.readableBytes()];
		cb2.readBytes(read2);
		assert new String(read2).equals("Hello world");

		assert cb2.release();
		assert b1.refCnt() == 1;
		assert cb2.refCnt() == 0;

		//So it appears that we can indeed dispose of composite byte bufs without disposing of their components
	}

	@Test
	public void WrappedTest() {

		ByteBuf b1 = Unpooled.buffer();
		b1.writeBytes("Hello ".getBytes());


		ByteBuf b2 = Unpooled.buffer();
		b2.writeBytes("world".getBytes());

		ByteBuf wrapped1 = Unpooled.wrappedBuffer(b1, b2);
		byte[] bytes1 = new byte[wrapped1.readableBytes()];
		wrapped1.readBytes(bytes1);
		assert b1.readerIndex() == 0;
		assert new String(bytes1).equals("Hello world");

		ByteBuf wrapped2 = Unpooled.wrappedBuffer(b1, b2);
		byte[] bytes2 = new byte[wrapped2.readableBytes()];
		wrapped2.readBytes(bytes2);
		assert b1.readerIndex() == 0;
		assert new String(bytes2).equals("Hello world");

	}


	@Test
	public void SliceTest() {

		ByteBuf b1 = Unpooled.buffer();
		b1.writeBytes("Hello world".getBytes());

		ByteBuf slice = b1.slice(0, 5).asReadOnly().retain();
		assert b1.refCnt() == 1;
		assert slice.refCnt() == 1;
		assert slice.release();
		assert b1.refCnt() == 0;
	}

	@Test
	public void CompositeFromCompositeComponentsTest(){
		CompositeByteBuf log = Unpooled.compositeBuffer(Integer.MAX_VALUE);

		for(int i = 0; i < 5; i++){
			ByteBuf b = Unpooled.buffer();
			b.writeBytes("Hello world".getBytes());
			assert b.refCnt() == 1;
			log.addComponent(true, b);
			assert b.refCnt() == 1;
		}
		assert log.internalComponent(1).refCnt() == 1;
		assert log.refCnt() == 1;

		//delta is going to be from 5 - 30, including 6 bytes from first, 11 from second and 8 from third bufs
		//"Hello worldHello worldHello worldHello worldHello world"
		//"_____ worldHello worldHello w__________________________
		CompositeByteBuf delta = buildDelta(log, 16, 41-16);

		assert delta.refCnt() == 1 ;
		System.out.println(log.internalComponent(1).refCnt());
		assert log.internalComponent(1).refCnt() == 2;

		byte[] bytes = new byte[delta.readableBytes()];
		delta.readBytes(bytes);
		System.out.println(new String(bytes));
		assert new String(bytes).equals(" worldHello worldHello wo"); //o?
		delta.readerIndex(0);

		log.readerIndex(12);
		log.discardReadComponents();
		assert log.capacity() == 44;

		delta.readBytes(bytes);
		System.out.println(new String(bytes));
		assert new String(bytes).equals(" worldHello worldHello wo"); //o?

		delta.release();
		System.out.println(log.internalComponent(1).refCnt());
		assert log.internalComponent(1).refCnt() == 1;


	}

	private static CompositeByteBuf buildDelta(CompositeByteBuf log, int startIndex, int length){

		CompositeByteBuf delta = Unpooled.compositeBuffer(Integer.MAX_VALUE);
		int numBytesLeft = length;
		int currIndex = startIndex;
		int bufferComponentSizes = log.internalComponent(0).capacity();
		while (numBytesLeft != 0){
			int bufferIndex = currIndex / bufferComponentSizes;
			ByteBuf buf = log.internalComponent(bufferIndex);
			int indexInBuffer = currIndex % bufferComponentSizes;
			int numBytesFromBuf = Math.min(numBytesLeft, bufferComponentSizes - indexInBuffer);
			delta.addComponent(true, buf.retainedSlice(indexInBuffer, numBytesFromBuf));
			currIndex+= numBytesFromBuf;
			numBytesLeft -= numBytesFromBuf;
		}

		return delta;

	}
}
