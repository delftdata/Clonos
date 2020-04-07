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
	CheckpointOffset latestNonCompletedCheckpoint;
	private int[] channelOffsets;

	private VertexId vertexBeingLogged;

	public CircularVertexCausalLog(int numDownstreamChannels, VertexId vertexBeingLogged) {
		this(DEFAULT_START_SIZE, numDownstreamChannels, vertexBeingLogged);
	}

	public CircularVertexCausalLog(int startSize, int numDownstreamChannels, VertexId vertexBeingLogged) {
		array = new byte[startSize];
		offsets = new LinkedList<>();
		start = 0;
		end = 0;
		size = 0;

		this.vertexBeingLogged = vertexBeingLogged;

		//initialized to 0s
		channelOffsets = new int[numDownstreamChannels];
	}


	@Override
	public byte[] getDeterminants() {
		byte[] copy = new byte[size];
		circularArrayCopyOutOfLog(array, start, end - start, copy);
		return copy;
	}

	@Override
	public void appendDeterminants(byte[] determinants) {
		while (!hasSpaceFor(determinants.length))
			grow();

		circularCopyIntoLog(determinants, 0);
	}

	@Override
	public void processUpstreamVertexCausalLogDelta(VertexCausalLogDelta vertexCausalLogDelta) {
		int numDeterminantsSinceLastMarker = end - (latestNonCompletedCheckpoint == null ? start : latestNonCompletedCheckpoint.offset);
		int newDeterminantsLength = vertexCausalLogDelta.offsetFromLastMarker - numDeterminantsSinceLastMarker;

		while (!hasSpaceFor(newDeterminantsLength))
			grow();


		if(newDeterminantsLength > 0)
			circularCopyIntoLog(vertexCausalLogDelta.logDelta, numDeterminantsSinceLastMarker);

	}


	@Override
	public void notifyCheckpointBarrier(long checkpointId) {
		latestNonCompletedCheckpoint = new CheckpointOffset(checkpointId, end);
		offsets.add(latestNonCompletedCheckpoint);
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

		if(latestNonCompletedCheckpoint.id == checkpointId)
			latestNonCompletedCheckpoint = null;
	}


	@Override
	public VertexCausalLogDelta getNextDeterminantsForDownstream(int channel) {
		byte[] toReturn = new byte[circularDistance(channelOffsets[channel], end, array.length)];
		circularArrayCopyOutOfLog(array, channelOffsets[channel], end - channelOffsets[channel], toReturn);

		//offset has to be logical. It is the distance to the latest non completed marker.
		int offset = channelOffsets[channel] - (latestNonCompletedCheckpoint != null ? latestNonCompletedCheckpoint.offset : start);

		channelOffsets[channel] = end; //channel has all the latest determinants

		return new VertexCausalLogDelta(vertexBeingLogged, toReturn, offset);
	}


	private static int circularDistance(int from, int to, int totalSize) {
		return (to - from) % totalSize;
	}

	private void circularArrayCopyOutOfLog(byte[] from, int start, int numBytesToCopy, byte[] to) {
		if (start + numBytesToCopy <= from.length) {
			System.arraycopy(from, start, to, 0, numBytesToCopy);
		} else { //Goes around the circle
			int numBytesToCopyBeforeCompleteCircle = from.length - start;
			System.arraycopy(from, start, to, 0, numBytesToCopyBeforeCompleteCircle);
			System.arraycopy(from, start, to, numBytesToCopyBeforeCompleteCircle, numBytesToCopy - numBytesToCopyBeforeCompleteCircle);
		}
	}

	/**
	 * Grows the array and moves everything to position 0
	 */
	private void grow() {
		byte[] newArray = new byte[array.length * GROWTH_FACTOR];

		circularArrayCopyOutOfLog(array, start, size, newArray);
		int move = -start;
		start = 0;
		end = size;

		// Since we reset to 0, we need to move the offsets as well.
		for(CheckpointOffset offset : offsets) {
			offset.offset = (offset.offset + move) % array.length;
		}

		for(int channelIndex = 0 ; channelIndex < channelOffsets.length; channelIndex++){
			channelOffsets[channelIndex] = (channelOffsets[channelIndex] + move) % array.length;
		}

		array = newArray;
	}

	private boolean hasSpaceFor(int toAdd) {
		return array.length - size >= toAdd;
	}

	/**
	 * PRECONDITION: has enough space!
	 * @param determinants
	 * @param offset
	 */
	private void circularCopyIntoLog(byte[] determinants, int offset) {
		if (end >= start) {
			int bytesTillLoop = array.length - end;
			if (determinants.length - offset > bytesTillLoop) {
				System.arraycopy(determinants, offset, array, end, bytesTillLoop);
				System.arraycopy(determinants, offset + bytesTillLoop, array, 0, determinants.length - offset - bytesTillLoop);

			} else {
				System.arraycopy(determinants, offset, array, end, determinants.length-offset);
			}
		} else {
			System.arraycopy(determinants, offset, array, end, determinants.length - offset);
		}
		end = (end + determinants.length - offset) % array.length;
		size += determinants.length - offset;
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
