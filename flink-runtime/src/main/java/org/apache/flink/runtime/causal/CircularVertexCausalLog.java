package org.apache.flink.runtime.causal;


import java.util.*;

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
	private Map<VertexId, Integer> vertexOffsets;


	public CircularVertexCausalLog(int startSize, Collection<VertexId> downstreamVertexIds) {
		array = new byte[startSize];
		offsets = new LinkedList<>();
		start = 0;
		end = 0;
		size = 0;

		vertexOffsets = new HashMap<VertexId, Integer>();
		for (VertexId id : downstreamVertexIds) vertexOffsets.put(id, 0);
	}

	public CircularVertexCausalLog(Collection<VertexId> downstreamVertexIds) {
		this(DEFAULT_START_SIZE, downstreamVertexIds);
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
				if(determinants.length > bytesTillLoop) {
					System.arraycopy(determinants, 0, array, end, bytesTillLoop);
					System.arraycopy(determinants, bytesTillLoop, array, 0, determinants.length - bytesTillLoop);

				}else {
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
	public void notifyDownstreamFailure(VertexId vertexId) {
		vertexOffsets.put(vertexId, start);
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
	public byte[] getNextDeterminantsForDownstream(VertexId vertexId) {
		byte[] toReturn = new byte[circularDistance(vertexOffsets.get(vertexId), end, array.length)];
		circularArrayCopy(array, vertexOffsets.get(vertexId), end, array.length, toReturn);
		vertexOffsets.put(vertexId, end);
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
