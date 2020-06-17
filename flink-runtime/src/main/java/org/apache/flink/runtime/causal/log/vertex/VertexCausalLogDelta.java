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

package org.apache.flink.runtime.causal.log.vertex;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.causal.VertexID;
import org.apache.flink.runtime.causal.log.NettyMessageWritable;
import org.apache.flink.runtime.causal.log.thread.SubpartitionThreadLogDelta;
import org.apache.flink.runtime.causal.log.thread.ThreadLogDelta;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class VertexCausalLogDelta implements NettyMessageWritable, IOReadableWritable {

	/**
	 * The {@link VertexID} of the vertex that this delta refers to
	 */
	VertexID vertexID;

	ThreadLogDelta mainThreadDelta;

	SortedMap<IntermediateResultPartitionID, SortedMap<Integer, SubpartitionThreadLogDelta>> partitionDeltas;

	Logger LOG = LoggerFactory.getLogger(VertexCausalLogDelta.class);

	public VertexCausalLogDelta() {
		this(null);
	}

	public VertexCausalLogDelta(VertexID vertexID) {
		this.vertexID = vertexID;
		partitionDeltas = new TreeMap<>();
	}

	/**
	 * When creatign a VertexCausalLogDelta pass null for mainThreadDelta if no update and an empty map for the subpartition deltas if no updates.
	 */
	public VertexCausalLogDelta(VertexID vertexID, ThreadLogDelta mainThreadDelta, Map<IntermediateResultPartitionID, Map<Integer, SubpartitionThreadLogDelta>> partitionDeltas) {
		this.vertexID = vertexID;
		this.mainThreadDelta = mainThreadDelta;
		this.partitionDeltas = new TreeMap<>();

		//sort the entries.
		for (Map.Entry<IntermediateResultPartitionID, Map<Integer, SubpartitionThreadLogDelta>> e : partitionDeltas.entrySet()) {
			this.partitionDeltas.put(e.getKey(), new TreeMap<>(e.getValue()));
		}
	}

	public VertexID getVertexId() {
		return vertexID;
	}

	public void setVertexId(VertexID vertexId) {
		this.vertexID = vertexId;
	}


	public ThreadLogDelta getMainThreadDelta() {
		return mainThreadDelta;
	}

	public SortedMap<IntermediateResultPartitionID, SortedMap<Integer, SubpartitionThreadLogDelta>> getPartitionDeltas() {
		return partitionDeltas;
	}

	public boolean hasUpdates() {
		return mainThreadDelta != null || partitionDeltas.values().size() > 0;
	}

	@Override
	public int getHeaderSize() {
		// VertexID (2), hasMain (1), numPartitionUpdates (2), if has main MainThreadLogDelta header Size (8),
		// For each partition DataSetID(16) and numSubpartUpdates(2), for each subpartition update (2 + 4 + 4)
		return 2 + 1 + 2 + (mainThreadDelta == null ? 0 : 2 * 4) + partitionDeltas.size() * (16 + 2)
			+ partitionDeltas.values().stream().mapToInt(x -> x.values().size()).sum() * (Integer.BYTES * 2 + Short.BYTES);
	}

	@Override
	public int getBodySize() {
		return (mainThreadDelta != null ? mainThreadDelta.getDeltaSize() : 0) +
			partitionDeltas.values().stream().flatMap(x -> x.values().stream())
				.mapToInt(SubpartitionThreadLogDelta::getDeltaSize).sum();
	}

	@Override
	public void writeHeaderTo(ByteBuf byteBuf) {
		byteBuf.writeShort(vertexID.getVertexId());
		byteBuf.writeBoolean(mainThreadDelta != null);
		byteBuf.writeShort(partitionDeltas.size());

		if (mainThreadDelta != null) {
			byteBuf.writeInt(mainThreadDelta.getOffsetFromEpoch());
			byteBuf.writeInt(mainThreadDelta.getRawDeterminants().readableBytes());
		}

		for (Map.Entry<IntermediateResultPartitionID, SortedMap<Integer, SubpartitionThreadLogDelta>> entry : partitionDeltas.entrySet()) {
			byteBuf.writeLong(entry.getKey().getUpperPart());
			byteBuf.writeLong(entry.getKey().getLowerPart());
			byteBuf.writeShort(entry.getValue().size());
			for (SubpartitionThreadLogDelta subpartitionLogDelta : entry.getValue().values()) {
				byteBuf.writeShort(subpartitionLogDelta.getSubpartitionIndex());
				byteBuf.writeInt(subpartitionLogDelta.getOffsetFromEpoch());
				byteBuf.writeInt(subpartitionLogDelta.getRawDeterminants().readableBytes());
			}
		}
	}

	@Override
	public void writeBodyTo(CompositeByteBuf byteBuf) {
		if (mainThreadDelta != null)
			byteBuf.addComponent(true, mainThreadDelta.getRawDeterminants());
		for (Map<Integer, SubpartitionThreadLogDelta> partitionlogs : partitionDeltas.values())
			for (SubpartitionThreadLogDelta delta : partitionlogs.values())
				byteBuf.addComponent(true, delta.getRawDeterminants());
	}

	@Override
	public void readHeaderFrom(ByteBuf byteBuf) {
		this.vertexID = new VertexID(byteBuf.readShort());
		boolean hasMainThread = byteBuf.readBoolean();
		short numPartitionUpdates = byteBuf.readShort();

		this.partitionDeltas = new TreeMap<>();

		if (hasMainThread) {
			int mainThreadOffset = byteBuf.readInt();
			int mainThreadNumBytes = byteBuf.readInt();
			mainThreadDelta = new ThreadLogDelta(mainThreadOffset, mainThreadNumBytes);
		}

		for (int p = 0; p < numPartitionUpdates; p++) {
			long upperPartID = byteBuf.readLong();
			long lowerPartID = byteBuf.readLong();
			IntermediateResultPartitionID intermediateResultPartitionID = new IntermediateResultPartitionID(lowerPartID, upperPartID);
			TreeMap<Integer, SubpartitionThreadLogDelta> partitionMap = new TreeMap<>();
			partitionDeltas.put(intermediateResultPartitionID, partitionMap);

			short numSubpartitionUpdates = byteBuf.readShort();

			for (int s = 0; s < numSubpartitionUpdates; s++) {
				int subpartitionIndex = byteBuf.readShort();
				int subpartOffsetFromEpoch = byteBuf.readInt();
				int subPartBufSize = byteBuf.readInt();

				partitionMap.put(subpartitionIndex, new SubpartitionThreadLogDelta(subpartOffsetFromEpoch, subpartitionIndex, subPartBufSize));

			}
		}
	}

	@Override
	public void readBodyFrom(ByteBuf byteBuf) {
		if (mainThreadDelta != null)
			mainThreadDelta.setRawDeterminants(byteBuf.readRetainedSlice(mainThreadDelta.getDeltaSize()));
		for (Map.Entry<IntermediateResultPartitionID, SortedMap<Integer, SubpartitionThreadLogDelta>> pe : partitionDeltas.entrySet()) {
			for (Map.Entry<Integer, SubpartitionThreadLogDelta> se : pe.getValue().entrySet()) {
				se.getValue().setRawDeterminants(byteBuf.readRetainedSlice(se.getValue().getDeltaSize()));
			}
		}
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeShort(vertexID.getVertexId());
		out.writeBoolean(mainThreadDelta != null);
		out.writeShort(partitionDeltas.size());

		if (mainThreadDelta != null) {
			out.writeInt(mainThreadDelta.getOffsetFromEpoch());
			out.writeInt(mainThreadDelta.getRawDeterminants().readableBytes());
		}

		for (Map.Entry<IntermediateResultPartitionID, SortedMap<Integer, SubpartitionThreadLogDelta>> entry : partitionDeltas.entrySet()) {
			out.writeLong(entry.getKey().getUpperPart());
			out.writeLong(entry.getKey().getLowerPart());
			out.writeShort(entry.getValue().size());
			for (SubpartitionThreadLogDelta subpartitionLogDelta : entry.getValue().values()) {
				out.writeShort(subpartitionLogDelta.getSubpartitionIndex());
				out.writeInt(subpartitionLogDelta.getOffsetFromEpoch());
				out.writeInt(subpartitionLogDelta.getRawDeterminants().readableBytes());
			}
		}

		if (mainThreadDelta != null) {
			byte[] copy = new byte[mainThreadDelta.getDeltaSize()];
			mainThreadDelta.getRawDeterminants().readBytes(copy);
			out.write(copy); //todo if possible we should avoid this data copy, however it isnt critical path
		}

		for (Map.Entry<IntermediateResultPartitionID, SortedMap<Integer, SubpartitionThreadLogDelta>> entry : partitionDeltas.entrySet()) {
			for (SubpartitionThreadLogDelta subpartitionLogDelta : entry.getValue().values()) {
				byte[] copy = new byte[subpartitionLogDelta.getDeltaSize()];
				subpartitionLogDelta.getRawDeterminants().readBytes(copy);
				out.write(copy); //todo if possible we should avoid this data copy, however it isnt critical path
			}
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.vertexID = new VertexID(in.readShort());
		boolean hasMainThread = in.readBoolean();
		short numPartitionUpdates = in.readShort();

		this.partitionDeltas = new TreeMap<>();

		if (hasMainThread) {
			int mainThreadOffset = in.readInt();
			int mainThreadNumBytes = in.readInt();
			mainThreadDelta = new ThreadLogDelta(mainThreadOffset, mainThreadNumBytes);
		}

		for (int p = 0; p < numPartitionUpdates; p++) {
			long upperPartID = in.readLong();
			long lowerPartID = in.readLong();
			IntermediateResultPartitionID intermediateResultPartitionID = new IntermediateResultPartitionID(lowerPartID, upperPartID);
			TreeMap<Integer, SubpartitionThreadLogDelta> partitionMap = new TreeMap<>();
			partitionDeltas.put(intermediateResultPartitionID, partitionMap);

			short numSubpartitionUpdates = in.readShort();

			for (int s = 0; s < numSubpartitionUpdates; s++) {
				int subpartitionIndex = in.readShort();
				int subpartOffsetFromEpoch = in.readInt();
				int subPartBufSize = in.readInt();

				partitionMap.put(subpartitionIndex, new SubpartitionThreadLogDelta(subpartOffsetFromEpoch, subpartitionIndex, subPartBufSize));

			}
		}

		//read header

		if (mainThreadDelta != null) {
			byte[] toWrap = new byte[mainThreadDelta.getDeltaSize()];
			in.read(toWrap);
			mainThreadDelta.setRawDeterminants(Unpooled.wrappedBuffer(toWrap));
		}
		for (Map.Entry<IntermediateResultPartitionID, SortedMap<Integer, SubpartitionThreadLogDelta>> pe : partitionDeltas.entrySet()) {
			for (Map.Entry<Integer, SubpartitionThreadLogDelta> se : pe.getValue().entrySet()) {
				byte[] toWrap = new byte[se.getValue().getDeltaSize()];
				in.read(toWrap);
				se.getValue().setRawDeterminants(Unpooled.wrappedBuffer(toWrap));
			}
		}
	}

	public void merge(VertexCausalLogDelta that) {
		mergeMains(that);
		mergePartitions(that);
	}

	private void mergeMains(VertexCausalLogDelta that) {
		if (this.mainThreadDelta == null && that.mainThreadDelta != null)
			this.mainThreadDelta = that.mainThreadDelta;
		else if (this.mainThreadDelta != null && that.mainThreadDelta != null && this.mainThreadDelta.getDeltaSize() < that.mainThreadDelta.getDeltaSize()) {
			this.mainThreadDelta.getRawDeterminants().release();
			this.mainThreadDelta = that.mainThreadDelta;
		} else if (that.mainThreadDelta != null)
			that.getMainThreadDelta().getRawDeterminants().release();
	}

	private void mergePartitions(VertexCausalLogDelta that) {
		if (this.partitionDeltas == null) {
			//If we do not yet have partition deltas, take the subpartition deltas of the other.
			this.partitionDeltas = that.partitionDeltas;
		} else {
			//If we both have partition deltas, merge them partition by partition
			//i.e. if we dont have a partition, but they do, take it, otherwise merge it subpartition per subpartition.
			for (Map.Entry<IntermediateResultPartitionID, SortedMap<Integer, SubpartitionThreadLogDelta>> thatPartitionEntry : that.partitionDeltas.entrySet()) {
				if (!this.partitionDeltas.containsKey(thatPartitionEntry.getKey())) {
					this.partitionDeltas.put(thatPartitionEntry.getKey(), thatPartitionEntry.getValue());
				} else {
					Map<Integer, SubpartitionThreadLogDelta> ourPartitionEntry = this.partitionDeltas.get(thatPartitionEntry.getKey());
					for (Map.Entry<Integer, SubpartitionThreadLogDelta> thatSubpartitionEntry : thatPartitionEntry.getValue().entrySet()) {

						if (!ourPartitionEntry.containsKey(thatSubpartitionEntry.getKey()))
							ourPartitionEntry.put(thatSubpartitionEntry.getKey(), thatSubpartitionEntry.getValue());
						else {
							SubpartitionThreadLogDelta ourDelta = ourPartitionEntry.get(thatSubpartitionEntry.getKey());
							SubpartitionThreadLogDelta theirDelta = thatSubpartitionEntry.getValue();
							if (ourDelta.getDeltaSize() > theirDelta.getDeltaSize()) {
								theirDelta.getRawDeterminants().release();
							} else {
								ourDelta.getRawDeterminants().release();
								ourDelta.setRawDeterminants(theirDelta.getRawDeterminants());
							}
						}

					}
				}
			}
		}
	}

	@Override
	public String toString() {
		return "VertexCausalLogDelta{" +
			"vertexId=" + vertexID +
			", mainThreadDelta=" + mainThreadDelta +
			", partitionDeltas=" + partitionDeltas +
			'}';
	}

	public void release() {
		boolean mainDestroyed = true;
		if (mainThreadDelta != null)
			mainDestroyed = mainThreadDelta.getRawDeterminants().release();
		List<Boolean> destroyed = partitionDeltas.values().stream().flatMap(m -> m.values().stream()).map(s -> s.getRawDeterminants().release()).collect(Collectors.toList());

		LOG.debug("Call to release. Main destroyed: {}, Subpart destroyed: {}", mainDestroyed, "[" + destroyed.stream().map(Object::toString).collect(Collectors.joining(", ")) + "]");
	}
}
