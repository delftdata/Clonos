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


import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Queue;

/**
 * Encoding strategy for determinants. Takes a determinant and returns its representation as bytes.
 * The tag must always be written first, as this allows the determinant to later be decoded.
 */
public interface DeterminantEncoder {

	byte[] encode(Determinant determinant);

	void encodeTo(Determinant determinant, ByteBuf targetBuf);

	Determinant decodeNext(ByteBuf buffer);

    Determinant decodeNext(ByteBuf b, Queue<Determinant>[] determinantCache);

	Determinant decodeOrderDeterminant(ByteBuf b);

	Determinant decodeOrderDeterminant(ByteBuf b, OrderDeterminant reuse);

	Determinant decodeTimestampDeterminant(ByteBuf b);

	Determinant decodeTimestampDeterminant(ByteBuf b, TimestampDeterminant reuse);

	Determinant decodeRNGDeterminant(ByteBuf b);

	Determinant decodeRNGDeterminant(ByteBuf b, RNGDeterminant reuse);

	Determinant decodeBufferBuiltDeterminant(ByteBuf b);

	Determinant decodeBufferBuiltDeterminant(ByteBuf b, BufferBuiltDeterminant reuse);

	Determinant decodeTimerTriggerDeterminant(ByteBuf b);

	Determinant decodeTimerTriggerDeterminant(ByteBuf b, TimerTriggerDeterminant reuse);

	Determinant decodeSourceCheckpointDeterminant(ByteBuf b);

	Determinant decodeSourceCheckpointDeterminant(ByteBuf b, SourceCheckpointDeterminant reuse);
}
