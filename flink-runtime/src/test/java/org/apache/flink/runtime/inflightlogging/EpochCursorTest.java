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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.junit.Test;

import java.util.SortedMap;
import java.util.TreeMap;

public class EpochCursorTest {

	//public NetworkBuffer getBuffer(int data){
	//	MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(4);
	//	NetworkBuffer buffer = new NetworkBuffer(segment,
	//		FreeingBufferRecycler.INSTANCE);

	//	buffer.writeInt(data);
	//	return buffer;
	//}

	//@Test
	//public void testEpochCursor(){
	//	SpilledReplayIterator.EpochCursor cursor = getEpochCursor();

	//	cursor.next();
	//	cursor.previous();
	//	assert cursor.hasNext();

	//	assert cursor.getRemaining() == 9;
	//	assert cursor.getNextEpochOffset() == 0;
	//	assert cursor.getNextEpoch() == 0;

	//	assert cursor.hasNext();
	//	assert cursor.next().asByteBuf().readInt() == 0;

	//	cursor.next();
	//	cursor.previous();
	//	assert cursor.hasNext();

	//	assert cursor.getRemaining() == 8;
	//	assert cursor.getNextEpochOffset() == 1;
	//	assert cursor.getNextEpoch() == 0;

	//	assert cursor.hasNext();
	//	assert cursor.next().asByteBuf().readInt() == 1;

	//	assert cursor.hasNext();
	//	assert cursor.next().asByteBuf().readInt() == 2;


	//	cursor.next();
	//	cursor.previous();
	//	assert cursor.hasNext();

	//	assert cursor.getRemaining() == 6;
	//	assert cursor.getNextEpochOffset() == 0;
	//	assert cursor.getNextEpoch() == 1;

	//	assert cursor.hasNext();
	//	assert cursor.next().asByteBuf().readInt() == 3;


	//	cursor.next();
	//	cursor.previous();
	//	assert cursor.hasNext();

	//	assert cursor.getRemaining() == 5;
	//	assert cursor.getNextEpochOffset() == 1;
	//	assert cursor.getNextEpoch() == 1;

	//	assert cursor.hasNext();
	//	assert cursor.next().asByteBuf().readInt() == 4;

	//	assert cursor.hasNext();
	//	assert cursor.next().asByteBuf().readInt() == 5;

	//	assert cursor.hasNext();
	//	assert cursor.next().asByteBuf().readInt() == 6;

	//	assert cursor.hasNext();
	//	assert cursor.next().asByteBuf().readInt() == 7;

	//	assert cursor.hasNext();
	//	assert cursor.next().asByteBuf().readInt() == 8;

	//	assert !cursor.hasNext();
	//}

	//@Test
	//public void testAddingWhileReplaying(){
	//	SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> log = new TreeMap<>();
	//	SpillableSubpartitionInFlightLogger.Epoch epoch0 = new SpillableSubpartitionInFlightLogger.Epoch(null, 0);
	//	epoch0.append(getBuffer(0));
	//	epoch0.append(getBuffer(1));
	//	epoch0.append(getBuffer(2));

	//	log.put(0L, epoch0);
	//	SpilledReplayIterator.EpochCursor cursor = new SpilledReplayIterator.EpochCursor(log);

	//	cursor.next();
	//	cursor.next();

	//	log.get(0L).append(getBuffer(3));
	//	cursor.notifyNewBuffer(0);

	//	cursor.next();
	//	assert cursor.hasNext();

	//	cursor.next();

	//	assert !cursor.hasNext();

	//	log.get(0L).append(getBuffer(4));
	//	cursor.notifyNewBuffer(0);

	//	assert cursor.hasNext();
	//	assert cursor.next().asByteBuf().readInt() == 4;
	//	assert !cursor.hasNext();

	//	SpillableSubpartitionInFlightLogger.Epoch epoch1 = new SpillableSubpartitionInFlightLogger.Epoch(null, 1);
	//	epoch1.append(getBuffer(5));
	//	log.put(1L, epoch1);
	//	cursor.notifyNewBuffer(1);

	//	assert cursor.hasNext();
	//	assert cursor.next().asByteBuf().readInt() == 5;
	//	assert !cursor.hasNext();


	//}

	//private SpilledReplayIterator.EpochCursor getEpochCursor() {
	//	SortedMap<Long, SpillableSubpartitionInFlightLogger.Epoch> log = new TreeMap<>();
	//	SpillableSubpartitionInFlightLogger.Epoch epoch0 = new SpillableSubpartitionInFlightLogger.Epoch(null, 0);
	//	epoch0.append(getBuffer(0));
	//	epoch0.append(getBuffer(1));
	//	epoch0.append(getBuffer(2));

	//	SpillableSubpartitionInFlightLogger.Epoch epoch1 = new SpillableSubpartitionInFlightLogger.Epoch(null, 0);
	//	epoch1.append(getBuffer(3));
	//	epoch1.append(getBuffer(4));
	//	epoch1.append(getBuffer(5));

	//	SpillableSubpartitionInFlightLogger.Epoch epoch2 = new SpillableSubpartitionInFlightLogger.Epoch(null, 0);
	//	epoch2.append(getBuffer(6));
	//	epoch2.append(getBuffer(7));
	//	epoch2.append(getBuffer(8));

	//	log.put(0L, epoch0);
	//	log.put(1L, epoch1);
	//	log.put(2L, epoch2);

	//	return new SpilledReplayIterator.EpochCursor(log);
	//}
}
