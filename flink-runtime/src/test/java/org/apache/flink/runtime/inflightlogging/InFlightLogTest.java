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

package org.apache.flink.runtime.inflightlogging;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class InFlightLogTest {

	@Test
	public void iteratorCountTest(){
		InFlightLog log = new InMemorySubpartitionInFlightLogger();
		populate(log);

		InFlightLogIterator<Buffer> iterator = log.getInFlightIterator(0);

		assertEquals(15 + 3, iterator.numberRemaining());
	}

	@Test
	public void logCheckpointCompleteTest() throws Exception {
		InFlightLog log = new InMemorySubpartitionInFlightLogger();
		populate(log);

		log.notifyCheckpointComplete(1);
		InFlightLogIterator<Buffer> iterator = log.getInFlightIterator(0);

		assertEquals(10 + 2, iterator.numberRemaining());
	}

	@Test
	public void logIterationTest() throws Exception {
		InFlightLog log = new InMemorySubpartitionInFlightLogger();
		populate(log);

		log.notifyCheckpointComplete(1);
		InFlightLogIterator<Buffer> iterator = log.getInFlightIterator(0);
		assertEquals(true, iterator.hasNext());
	}

	private void populate(InFlightLog log) {
		for(int epoch = 0; epoch < 3; epoch++) {
			for(int bufferToProduce = 0; bufferToProduce < 5 ; bufferToProduce++) {

				Buffer buffer = new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(64),
					FreeingBufferRecycler.INSTANCE);
				log.log(buffer, epoch);
			}
			Buffer buffer = new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(64),
				FreeingBufferRecycler.INSTANCE);
			log.log(buffer, epoch);

		}
	}

	private void populateWithEmptyEpoch(InFlightLog log) {
		for(int epoch = 0; epoch < 3; epoch++) {
			if(epoch % 2 == 0) {
				for (int bufferToProduce = 0; bufferToProduce < 5; bufferToProduce++) {

					Buffer buffer = new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(64),
						FreeingBufferRecycler.INSTANCE);
					log.log(buffer, epoch);
				}
			}
			Buffer buffer = new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(64),
				FreeingBufferRecycler.INSTANCE);
			log.log(buffer, epoch);

		}
	}
}
