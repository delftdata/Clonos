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
import java.util.List;

public class LinkedListVertexCausalLog implements VertexCausalLog {


	private List<CheckpointSlice> log;
	Offset[] offsets;
	private static final long DEFAULT_SLICE_ID = 0;

	public LinkedListVertexCausalLog(int numDownstreamChannels) {
		log = new LinkedList<>();
		log.add(new CheckpointSlice(DEFAULT_SLICE_ID));
		offsets = new Offset[numDownstreamChannels];
		for (int i = 0; i < numDownstreamChannels; i++) {
			offsets[i] = new Offset(0, 0);
		}

	}

	@Override
	public byte[] getDeterminants() {
		int total = getTotalSize();
		byte[] toReturn = new byte[total];

		int curr = 0;
		for (CheckpointSlice slice : log) {
			for (byte[] arr : slice.determinants) {
				System.arraycopy(arr, 0, toReturn, curr, arr.length);
				curr += arr.length;
			}
		}
		return toReturn;
	}

	@Override
	public void appendDeterminants(byte[] determinants) {
		log.get(log.size() - 1).append(determinants);
	}

	@Override
	public void notifyCheckpointBarrier(long checkpointId) {
		log.add(new CheckpointSlice(checkpointId));
	}

	@Override
	public void notifyDownstreamFailure(int channel) {
		offsets[channel] = new Offset(0, 0);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		int index = 0;
		while (log.get(index).id <= checkpointId)
			index++;

		log = log.subList(index, log.size());

		for (Offset o : offsets)
			o.setSliceIndex(Math.max(0, o.getSliceIndex() - index));
	}

	@Override
	public byte[] getNextDeterminantsForDownstream(int channel) {

		Offset offset = offsets[channel];

		int total = log.get(offset.sliceIndex).getSize() - offset.sliceOffset;

		for (int i = offset.sliceIndex + 1; i < log.size(); i++)
			total += log.get(i).getSize();

		byte[] toReturn = new byte[total];

		List<byte[]> first = log.get(offset.sliceIndex).getDeterminants();
		int pos = 0;
		int index = 0;
		while (pos != offset.sliceOffset)
			pos += first.get(index++).length;

		int posInToReturn = 0;
		for (int i = index; i < first.size(); i++) {
			System.arraycopy(first.get(i), 0, toReturn, posInToReturn, first.get(i).length);
			posInToReturn += first.get(i).length;
		}

		for (int sliceIndex = offset.sliceIndex + 1; sliceIndex < log.size(); sliceIndex++) {
			List<byte[]> determinantsOfSlice = log.get(sliceIndex).getDeterminants();
			for (byte[] bytes : determinantsOfSlice) {
				System.arraycopy(bytes, 0, toReturn, posInToReturn, bytes.length);
				posInToReturn += bytes.length;
			}
		}

		offset.setSliceIndex(log.size()-1);
		offset.setSliceOffset(log.get(log.size()-1).getSize());


		return toReturn;
	}

	private int getTotalSize(){
		return log.stream().map(CheckpointSlice::getSize).reduce(0, Integer::sum);
	}

	private class Offset{
		private int sliceIndex;
		private int sliceOffset;

		public Offset(int sliceIndex, int sliceOffset) {
			this.sliceIndex = sliceIndex;
			this.sliceOffset = sliceOffset;
		}

		public int getSliceIndex() {
			return sliceIndex;
		}

		public void setSliceIndex(int sliceIndex) {
			this.sliceIndex = sliceIndex;
		}

		public int getSliceOffset() {
			return sliceOffset;
		}

		public void setSliceOffset(int sliceOffset) {
			this.sliceOffset = sliceOffset;
		}
	}

	private class CheckpointSlice {
		private long id;
		private List<byte[]> determinants;
		private int totalSize;

		public CheckpointSlice(long id) {
			this.id = id;
			this.determinants = new LinkedList<>();
			this.totalSize = 0;
		}

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public List<byte[]> getDeterminants() {
			return determinants;
		}

		public void append(byte[] determinant) {
			this.determinants.add(determinant);
			this.totalSize += determinant.length;
		}

		public int getSize(){
			return totalSize;
		}
	}
}
