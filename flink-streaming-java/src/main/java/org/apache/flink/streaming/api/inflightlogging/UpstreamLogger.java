package org.apache.flink.streaming.api.inflightlogging;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.*;

/**
 * An internal operator that solely serves as a state logging facility for persisting,
 * partitioning and restoring output logs for dataflow cycles consistently. To support concurrency,
 * logs are being sliced proportionally to the number of concurrent snapshots. This allows committed
 * output logs to be uniquely identified and cleared after each complete checkpoint.
 * <p>
 * The design is based on the following assumptions:
 * <p>
 * - A slice is named after a checkpoint ID. Checkpoint IDs are numerically ordered within an execution.
 * - Each checkpoint barrier arrives back in FIFO order, thus we discard log slices in respective FIFO order.
 * - Upon restoration the logger sorts sliced logs in the same FIFO order and returns an Iterable that
 * gives a singular view of the log.
 * <p>
 * TODO it seems that ListState.clear does not unregister state. We need to put a hook for that.
 *
 * @param <IN>
 */
public class UpstreamLogger<IN> extends AbstractStreamOperator<IN> implements OneInputStreamOperator<IN, IN> {

	private final StreamConfig config;

	private LinkedList<ListState<StreamRecord<IN>>> slicedLog = new LinkedList<>();

	private UpstreamLogger(StreamConfig config) {
		this.config = config;
	}

	public void logRecord(StreamRecord<IN> record) throws Exception {
		if (!slicedLog.isEmpty()) {
			slicedLog.getLast().add(record);
		}
	}

	public void createSlice(String sliceID) throws Exception {
		ListState<StreamRecord<IN>> nextSlice =
			getOperatorStateBackend().getOperatorState(new ListStateDescriptor<>(sliceID,
				config.<StreamRecord<IN>>getTypeSerializerOut(getUserCodeClassloader()))); // We dont need this if maintaing in memory
		slicedLog.addLast(nextSlice);
	}

	public void discardSlice() {
		ListState<StreamRecord<IN>> logToEvict = slicedLog.pollFirst();
		logToEvict.clear();
	}

	public Iterable<StreamRecord<IN>> getReplayLog() throws Exception {
		final List<String> logSlices = new ArrayList<>(getOperatorStateBackend().getRegisteredStateNames());
		Collections.sort(logSlices, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return Long.valueOf(o1).compareTo(Long.valueOf(o2));
			}
		});

		final List<Iterator<StreamRecord<IN>>> wrappedIterators = new ArrayList<>();
		for (String splitID : logSlices) {
			wrappedIterators.add(getOperatorStateBackend()
				.getOperatorState(new ListStateDescriptor<>(splitID,
					config.<StreamRecord<IN>>getTypeSerializerOut(getUserCodeClassloader()))).get().iterator());
		}

		if (wrappedIterators.size() == 0) {
			return new Iterable<StreamRecord<IN>>() {
				@Override
				public Iterator<StreamRecord<IN>> iterator() {
					return Collections.emptyListIterator();
				}
			};
		}

		return new Iterable<StreamRecord<IN>>() {
			@Override
			public Iterator<StreamRecord<IN>> iterator() {

				return new Iterator<StreamRecord<IN>>() {
					int indx = 0;
					Iterator<StreamRecord<IN>> currentIterator = wrappedIterators.get(0);

					@Override
					public boolean hasNext() {
						if (!currentIterator.hasNext()) {
							progressLog();
						}
						return currentIterator.hasNext();
					}

					@Override
					public StreamRecord<IN> next() {
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
		for (String outputLogs : getOperatorStateBackend().getRegisteredStateNames()) {
			getOperatorStateBackend().getOperatorState(new ListStateDescriptor<>(outputLogs,
				config.<StreamRecord<IN>>getTypeSerializerOut(getUserCodeClassloader()))).clear();
		}
	}


	@Override
	public void open() throws Exception {
		super.open();
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<IN>> output) {
		super.setup(containingTask, config, output);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		//nothing to do
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		//nothing to do
	}

}
