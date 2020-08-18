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

package org.apache.flink.runtime.causal;

public class RemoteCausalLogTest {

	static byte[] toSend = getAlphaNumericString(50000).getBytes();

	int epochSize = 5000;

	// function to generate a random string of length n
	static String getAlphaNumericString(int n) {

		// chose a Character random from this String
		String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
			+ "0123456789"
			+ "abcdefghijklmnopqrstuvxyz";

		// create StringBuffer size of AlphaNumericString
		StringBuilder sb = new StringBuilder(n);

		for (int i = 0; i < n; i++) {

			// generate a random number between
			// 0 to AlphaNumericString variable length
			int index
				= (int) (AlphaNumericString.length()
				* Math.random());

			// add Character one by one in end of sb
			sb.append(AlphaNumericString
				.charAt(index));
		}

		return sb.toString();
	}

	//@Test
	//public void remoteCausalLogTest() throws InterruptedException {

	//	System.out.println(new String(toSend));
	//	System.out.println("---------------");
	//	VertexId vertexId = new VertexId((short) 0);
	//	UpstreamThreadCausalLog upstreamCausalLog = new ReplicatedThreadCausalLog(vertexId);

	//	int numWriterThreads = 10;
	//	int numReaderThreads = 10;

	//	List<WriterThread> writers = new ArrayList<>(numWriterThreads);
	//	List<ReaderThread> readers = new ArrayList<>(numReaderThreads);

	//	for (int i = 0; i < numWriterThreads; i++) {
	//		WriterThread thread = new WriterThread(vertexId, upstreamCausalLog);
	//		writers.add(thread);
	//		thread.start();
	//		Thread.sleep(1L);
	//	}
	//	Thread.sleep(500);

	//	for (int i = 0; i < numReaderThreads; i++) {
	//		ReaderThread thread = new ReaderThread(vertexId, upstreamCausalLog);
	//		readers.add(thread);
	//		thread.start();
	//		Thread.sleep(3L);
	//	}


	//	for (WriterThread writerThread : writers)
	//		writerThread.join();
	//	for (ReaderThread readerThread : readers)
	//		readerThread.join();


	//}

	//public class WriterThread extends Thread {


	//	private final VertexId vertexId;
	//	private final UpstreamThreadCausalLog upstreamCausalLog;
	//	private long checkpoint;
	//	private int readIndex;
	//	private int epochOffset;

	//	public WriterThread(VertexId vertexId, UpstreamThreadCausalLog upstreamCausalLog) {
	//		this.vertexId = vertexId;
	//		this.upstreamCausalLog = upstreamCausalLog;

	//		readIndex = 0;
	//		checkpoint = 0L;
	//		epochOffset = 0;
	//	}

	//	@Override
	//	public void run() {
	//		Random random = new Random(System.currentTimeMillis());
	//		while (readIndex != toSend.length) {
	//			while (epochOffset != epochSize) {
	//				int size = Math.min(epochSize - epochOffset, random.nextInt(1000));
	//				ByteBuf buf = Unpooled.buffer(size);
	//				buf.writeBytes(toSend, readIndex, size);
	//				upstreamCausalLog.processUpstreamVertexCausalLogDelta(new CausalLogThreadDelta(vertexId, buf, readIndex % epochSize), checkpoint);
	//				readIndex += size;
	//				epochOffset += size;
	//				try {
	//					Thread.sleep(random.nextInt(50));
	//				} catch (InterruptedException e) {
	//					e.printStackTrace();
	//				}
	//			}
	//			System.out.println("Done writing epoch " + checkpoint);
	//			checkpoint++;
	//			epochOffset = 0;
	//		}
	//	}
	//}

	//public class ReaderThread extends Thread {
	//	private final VertexId vertexId;
	//	private final UpstreamThreadCausalLog upstreamCausalLog;
	//	private byte[] readCopy;
	//	private int writeIndex;
	//	private InputChannelID channelID;
	//	private long checkpointID;

	//	public ReaderThread(VertexId vertexId, UpstreamThreadCausalLog upstreamCausalLog) {
	//		this.vertexId = vertexId;
	//		this.upstreamCausalLog = upstreamCausalLog;
	//		readCopy = new byte[toSend.length];
	//		this.writeIndex = 0;
	//		this.channelID = new InputChannelID();
	//		upstreamCausalLog.registerDownstreamConsumer(channelID);
	//		checkpointID = 0L;
	//	}


	//	@Override
	//	public void run() {
	//		int epochOffset = 0;
	//		while (writeIndex != readCopy.length) {
	//			while (epochOffset != epochSize) {
	//				CausalLogThreadDelta delta = upstreamCausalLog.getNextDeterminantsForDownstream(channelID, checkpointID);

	//				int readableBytes = delta.getRawDeterminants().readableBytes();
	//				delta.getRawDeterminants().readBytes(readCopy, writeIndex, readableBytes);

	//				writeIndex += readableBytes;
	//				epochOffset += readableBytes;
	//				System.out.println("Epoch: " + checkpointID + ", readable: " + readableBytes + ", writeIndex: " + writeIndex);

	//				try {
	//					Thread.sleep(120);
	//				} catch (InterruptedException e) {
	//					e.printStackTrace();
	//				}
	//			}
	//			System.out.println("Done reading epoch " + checkpointID);
	//			epochOffset = 0;
	//			checkpointID++;

	//		}
	//		System.out.println(new String(readCopy));
	//	}

	//}


}
