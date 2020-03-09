package org.apache.flink.runtime.causal;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class LinkedListLogTest {

	private static final String test_sentence_small = "Lorem ipsum "; //12 bytes
	private static final String test_sentence_large = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
		"Vivamus ac sapien ipsum. Curabitur sapien elit, commodo non quam non, aliquam tincidunt sem. " +
		"Curabitur tellus nulla, sagittis gravida cursus eget, facilisis sed augue." +
		" Cras a semper nisl, eu varius nisl. Aliquam aliquam et lectus ac pulvinar. Fusce tincidunt interdum metus. " +
		"Quisque a orci nisi. Nulla pulvinar dictum tortor sit amet ultricies. Cras et aliquam massa. " +
		"Mauris dignissim neque id finibus rhoncus. Aliquam pretium ac felis eu viverra. " +
		"Praesent vestibulum neque nec iaculis volutpat. Donec sagittis venenatis tortor, id viverra arcu tempor ac.";

	@Test
	public void growthTest() {

		List<VertexId> downstreamOperators = Collections.singletonList(new VertexId((short) 0));

		VertexCausalLog log = new LinkedListVertexCausalLog(downstreamOperators);

		for (int i = 0; i < 3; i++)
			log.appendDeterminants(test_sentence_small.getBytes()); //36 bytes. Causes one growth

		String expectedResult = test_sentence_small + test_sentence_small + test_sentence_small;
		assert (new String(log.getDeterminants()).equals(expectedResult));
		assert (new String(log.getNextDeterminantsForDownstream(new VertexId((short) 0))).equals(expectedResult));

	}


	@Test
	public void checkpointBarrierTest() throws Exception {

		List<VertexId> downstreamOperators = Arrays.asList(new VertexId((short) 0));

		VertexCausalLog log = new LinkedListVertexCausalLog(downstreamOperators);

		for (int i = 0; i < 2; i++)
			log.appendDeterminants(test_sentence_small.getBytes());

		log.notifyCheckpointBarrier(1);

		for (int i = 0; i < 3; i++)
			log.appendDeterminants(test_sentence_small.getBytes());

		log.notifyCheckpointComplete(1);

		String expectedResult = test_sentence_small + test_sentence_small + test_sentence_small;
		System.out.println(new String(log.getDeterminants()));
		assert (new String(log.getDeterminants()).equals(expectedResult));

	}

	@Test
	public void operatorDeterminantTrackingTest() {

		List<VertexId> downstreamOperators = Arrays.asList(new VertexId[]{new VertexId((short) 0), new VertexId((short) 1)});

		VertexCausalLog log = new LinkedListVertexCausalLog(downstreamOperators);

		assert (Arrays.equals(log.getNextDeterminantsForDownstream(new VertexId((short) 0)), new byte[0]));

		for (int i = 0; i < 2; i++)
			log.appendDeterminants(test_sentence_small.getBytes());

		String expectedResult = test_sentence_small + test_sentence_small;

		assert (new String(log.getNextDeterminantsForDownstream(new VertexId((short) 0))).equals(expectedResult));
		assert (new String(log.getNextDeterminantsForDownstream(new VertexId((short) 1))).equals(expectedResult));
		assert (new String(log.getNextDeterminantsForDownstream(new VertexId((short) 1))).equals(""));


		for (int i = 0; i < 3; i++)
			log.appendDeterminants(test_sentence_small.getBytes());


		expectedResult = test_sentence_small + test_sentence_small + test_sentence_small;

		assert (new String(log.getNextDeterminantsForDownstream(new VertexId((short) 0))).equals(expectedResult));
		assert (new String(log.getNextDeterminantsForDownstream(new VertexId((short) 1))).equals(expectedResult));
		assert (new String(log.getNextDeterminantsForDownstream(new VertexId((short) 1))).equals(""));

	}


	@Test
	public void performanceTest() throws InterruptedException {
		List<VertexId> downstreamOperators = Arrays.asList(new VertexId[]{new VertexId((short) 0), new VertexId((short) 1)});
		AtomicInteger count = new AtomicInteger(0);
		VertexCausalLog log = new LinkedListVertexCausalLog(downstreamOperators);
		Thread generator = new Thread(new Runnable() {
			@Override
			public void run() {
				Random r = new Random();
				byte[] arr = new byte[3];
				while (true) {
					r.nextBytes(arr);
					log.appendDeterminants(arr);
					count.incrementAndGet();
				}
			}
		});

		Thread checkpointInjector = new Thread(new Runnable() {
			@Override
			public void run() {
				int i = 1;
				while (true) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					log.notifyCheckpointBarrier(i);
				}
			}
		});
		Thread checkpointCompleter = new Thread(new Runnable() {
			@Override
			public void run() {
				int i = 1;
				try {
					Thread.sleep(150);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				while (true) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						log.notifyCheckpointComplete(i);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});

		generator.start();
		checkpointInjector.start();
		checkpointCompleter.start();

		int seconds = 30;
		long startTime = System.currentTimeMillis();
		int lastCount = 0;
		int newcount;
		while (System.currentTimeMillis() < startTime + seconds * 1000){

			newcount = count.get();


			System.out.println("Throughput: " +  (newcount - lastCount) + " determinants/sec");

			lastCount = newcount;

			Thread.sleep(1000);
		}
		generator.stop();
		checkpointCompleter.stop();
		checkpointInjector.stop();




	}
}
