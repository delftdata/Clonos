package org.apache.flink.runtime.inflightlogging;

import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.util.Iterator;

/**
 * An InFlightLog records {@link Buffer} instances which have been sent to other tasks.
 * It records where in the logs checkpoints happen, starting a new epoch.
 * The processing of a checkpoint barrier n starts an epoch n, however the barrier itself belongs in epoch n.
 */
public interface InFlightLog {

	public void log(Buffer buffer);

	public void logCheckpointBarrier(Buffer buffer, long checkpointId);

	public void clearLog();

	public void notifyCheckpointComplete(long checkpointId);

	public SizedListIterator<Buffer> getInFlightFromCheckpoint(long checkpointId);

}
