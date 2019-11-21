package org.apache.flink.runtime.causal;

import java.util.*;

public class LinkedListDeterminantLog implements DeterminantLog {


	private List<CheckpointSlice> log;
	private Map<String, Offset> operatorOffsets;

	public LinkedListDeterminantLog(List<String> operatorIds) {
		log = new LinkedList<>();
		this.operatorOffsets = new HashMap<>(operatorIds.size());
		for (String opId:  operatorIds) operatorOffsets.put(opId, new Offset(0,0));

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
		log.get(log.size()-1).determinants.add(determinants);
	}

	@Override
	public void notifyCheckpointBarrier(String checkpointId) {
		log.add(new CheckpointSlice(checkpointId));
	}

	@Override
	public void notifyCheckpointComplete(String checkpointId) {
		int index = 0;
		while(log.get(index).id.compareTo(checkpointId) < 1)
			index++;

		log = log.subList(index, log.size()-1);

		for(Offset o : operatorOffsets.values())
			o.setSliceIndex(Math.max(0, o.getSliceIndex() - index));
	}

	@Override
	public byte[] getNextDeterminantsForDownstream(String operatorId) {

		Offset offset = operatorOffsets.get(operatorId);

		int total = log.get(offset.sliceIndex).getSize() - offset.sliceOffset;

		for(int i = offset.sliceIndex; i < log.size(); i++)
			total += log.get(i).getSize();

		byte[] toReturn = new byte[total];

		List<byte[]> first = log.get(offset.sliceIndex).getDeterminants();
		int pos = 0;
		int index = 0;
		while(pos != offset.sliceOffset)
			pos += first.get(index++).length;

		int posInToReturn = 0;
		for(int i = index; i < first.size(); i++) {
			System.arraycopy(first.get(i), 0, toReturn, posInToReturn, first.get(i).length);
			posInToReturn += first.get(i).length;
		}

		for(int sliceIndex = offset.sliceIndex + 1; sliceIndex < log.size(); sliceIndex++){
			for(int i = 0; i < log.get(sliceIndex).getDeterminants().size(); i++) {
				System.arraycopy(log.get(sliceIndex).getDeterminants().get(i), 0, toReturn, posInToReturn, log.get(sliceIndex).getDeterminants().get(i).length);
				posInToReturn += log.get(sliceIndex).getDeterminants().get(i).length;
			}
		}

		offset.setSliceIndex(log.size()-1);
		offset.setSliceOffset(log.get(log.size()-1).determinants.size());

		return toReturn;
	}

	private int getTotalSize(){
		return log.stream().map(slice -> slice.getSize()).reduce(0, (a,b) -> a + b);
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

	private class CheckpointSlice{
		private String id;
		private List<byte[]> determinants;

		public CheckpointSlice(String id) {
			this.id = id;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public List<byte[]> getDeterminants() {
			return determinants;
		}

		public void setDeterminants(List<byte[]> determinants) {
			this.determinants = determinants;
		}

		public int getSize(){
			return determinants.stream().map(det -> det.length).reduce(0, (a,b) -> a + b);
		}
	}
}
