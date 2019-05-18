package org.apache.flink.streaming.api.inflightlogging;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.*;

public class InFlightLogger<OUT> {
	private SortedMap<String,StreamSlice<OUT>[]> slicedLog;
	private SortedMap<String, boolean[]> acknowledgements;

	private int numOutgoingChannels;


	public InFlightLogger(int numChannels){
		slicedLog = new TreeMap<>();
		this.numOutgoingChannels = numChannels;
		acknowledgements = new TreeMap<>();
	}

	public void logRecord(StreamRecord<OUT> record, int channelIndex) {
		if (!slicedLog.isEmpty()) {
			slicedLog.get(slicedLog.lastKey())[channelIndex].addRecord(record);
		}
	}

	public void createSlice(String barrierId){
		StreamSlice<OUT>[] newSlices = new StreamSlice[this.numOutgoingChannels];

		for(int i = 0; i < this.numOutgoingChannels; i++) newSlices[i] = new StreamSlice<>(barrierId);


		slicedLog.put(barrierId, newSlices);

		boolean[] newSliceAcknowledgements = new boolean[this.numOutgoingChannels];
		acknowledgements.put(barrierId, newSliceAcknowledgements);
	}

	public void addAcknowledgment(String barrierId, int outgoingChannelIndex) {
		boolean[] acknowledgementsOfBarrier = acknowledgements.get(barrierId);
		if(acknowledgementsOfBarrier == null) {
			//TODO can I throw exceptions? What is the protocol
		}
		acknowledgementsOfBarrier[outgoingChannelIndex] = true;

		// If all outgoing channels have acknowledged barrier, remove from log
		if(!Arrays.asList(acknowledgementsOfBarrier).contains(false)){
			if(!slicedLog.firstKey().equals(barrierId)){
				//TODO check if can throw exceptions
			}

			slicedLog.remove(barrierId);
			acknowledgements.remove(barrierId);
		}
	}

	public Iterable<StreamRecord<OUT>> getReplayLog(int outgoingChannelIndex) throws Exception {
		List<Iterator<StreamRecord<OUT>>> wrappedIterators = new ArrayList<>(slicedLog.keySet().size());

		for(String barrierId : slicedLog.keySet()) {
			StreamSlice<OUT> sliceOfChannel = slicedLog.get(barrierId)[outgoingChannelIndex];
			if(acknowledgements.get(barrierId)[outgoingChannelIndex] == false) //Only add to replay if outgoing channel hasnt acknowledged
				wrappedIterators.add(sliceOfChannel.getSliceRecords().iterator());
		}

		if (wrappedIterators.size() == 0) {
			return new Iterable<StreamRecord<OUT>>() {
				@Override
				public Iterator<StreamRecord<OUT>> iterator() {
					return Collections.emptyListIterator();
				}
			};
		}

		return new Iterable<StreamRecord<OUT>>() {
			@Override
			public Iterator<StreamRecord<OUT>> iterator() {

				return new Iterator<StreamRecord<OUT>>() {
					int indx = 0;
					Iterator<StreamRecord<OUT>> currentIterator = wrappedIterators.get(0);

					@Override
					public boolean hasNext() {
						if (!currentIterator.hasNext()) {
							progressLog();
						}
						return currentIterator.hasNext();
					}

					@Override
					public StreamRecord<OUT> next() {
						if (!currentIterator.hasNext() && indx < wrappedIterators.size()) {
							progressLog();
						}
						return currentIterator.next();
					}

					private void progressLog() {
						while (!currentIterator.hasNext() && ++indx < wrappedIterators.size()) {
							currentIterator = wrappedIterators.get(indx);
						}
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}

				};
			}
		};


	}



	public void clearLog() throws Exception {
		slicedLog  = new TreeMap<>();
		acknowledgements = new TreeMap<>();
	}

	private void discardSlice() {
		slicedLog.remove(slicedLog.firstKey());
		acknowledgements.remove(acknowledgements.firstKey());
		//TODO why did the other author clear the ListState? Just because it was persisted?
	}

	private void discardSlice(String barrierId) {
		slicedLog.remove(barrierId);
		acknowledgements.remove(barrierId);
		//TODO why did the other author clear the ListState? Just because it was persisted?
	}

}
