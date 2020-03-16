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
package org.apache.flink.runtime.causal;


import java.util.LinkedList;
import java.util.Queue;

/**
 * Implements the <link>UpstreamDeterminantCache</> as a Growable Circular Array
 */
public class CircularVertexCausalLog implements VertexCausalLog {

	private static final int DEFAULT_START_SIZE = 65536;
	private static final int GROWTH_FACTOR = 2;

	private byte[] array;
	private int start;
	private int end;
	private int size;

	private Queue<CheckpointOffset> offsets;
	private int[] channelOffsets;


	public CircularVertexCausalLog(int numDownstreamChannels) {
		this(DEFAULT_START_SIZE, numDownstreamChannels);
	}

	public CircularVertexCausalLog(int startSize, int numDownstreamChannels) {
		array = new byte[startSize];
		offsets = new LinkedList<>();
		start = 0;
		end = 0;
		size = 0;

		//initialized to 0s
		channelOffsets = new int[numDownstreamChannels];
	}


	@Override
	public byte[] getDeterminants() {
		byte[] copy = new byte[size];
		circularArrayCopy(array, start, end, array.length, copy);
		return copy;
	}

	@Override
	public void appendDeterminants(byte[] determinants) {
		if (!hasSpaceFor(determinants.length)) {
			grow();
			appendDeterminants(determinants);
		} else {
			if (end >= start) {
				int bytesTillLoop = array.length - end;
				if (determinants.length > bytesTillLoop) {
					System.arraycopy(determinants, 0, array, end, bytesTillLoop);
					System.arraycopy(determinants, bytesTillLoop, array, 0, determinants.length - bytesTillLoop);

				} else {
					System.arraycopy(determinants, 0, array, end, determinants.length);
				}
			} else {
				System.arraycopy(determinants, 0, array, end, determinants.length);
			}
			end = (end + determinants.length) % array.length;
			size += determinants.length;
		}
	}


	@Override
	public void notifyCheckpointBarrier(long checkpointId) {
		offsets.add(new CheckpointOffset(checkpointId, end));
		//record current position, as all records pertaining to this checkpoint are going to be between end
		// and next offsets end
	}

	@Override
	public void notifyDownstreamFailure(int channel) {
		channelOffsets[channel] = start;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		CheckpointOffset top;
		while (true) {
			top = offsets.peek();

			if (top == null)
				break;

			if (checkpointId >= top.id)
				offsets.poll();
			else
				break;
			size -= circularDistance(start, top.offset, array.length);
			start = top.offset; //delete everything pertaining to this epoch

		}

	}


	@Override
	public byte[] getNextDeterminantsForDownstream(int channel) {
		byte[] toReturn = new byte[circularDistance(channelOffsets[channel], end, array.length)];
		circularArrayCopy(array, channelOffsets[channel], end, array.length, toReturn);
		channelOffsets[channel] = end;
		return toReturn;
	}


	private int circularDistance(int from, int to, int totalSize) {
		if (to >= from) {
			return to - from;
		} else {
			return (totalSize - from) + to;
		}
	}

	private void circularArrayCopy(byte[] src, int start, int end, int size, byte[] to) {
		if (end >= start) {
			System.arraycopy(src, start, to, 0, end - start);
		} else {
			System.arraycopy(src, start, to, 0, size - start);
			System.arraycopy(src, start, to, size - start, end);
		}
	}

	private void grow() {
		byte[] newArray = new byte[array.length * GROWTH_FACTOR];
		System.arraycopy(array, 0, newArray, 0, array.length);
		array = newArray;
	}

	private boolean hasSpaceFor(int toAdd) {
		return array.length - size > toAdd;
	}


	private class CheckpointOffset {
		private long id;
		private int offset;

		public CheckpointOffset(long id, int offset) {
			this.id = id;
			this.offset = offset;
		}

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public int getOffset() {
			return offset;
		}

		public void setOffset(int offset) {
			this.offset = offset;
		}
	}


}
