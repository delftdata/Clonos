/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.causal;

import org.apache.flink.runtime.causal.determinant.RNGDeterminant;

import java.util.Random;

public class RandomService {

	private CausalLoggingManager causalLoggingManager;

	private Random random;


	public RandomService(CausalLoggingManager causalLoggingManager){
		this(causalLoggingManager, System.currentTimeMillis());
	}

	public RandomService(CausalLoggingManager causalLoggingManager, long seed) {
		this.causalLoggingManager = causalLoggingManager;
		this.random = new Random(seed);
	}



	public int nextInt() {
		return this.nextInt(Integer.MAX_VALUE);
	}

	public int nextInt(int maxExclusive) {
		if(causalLoggingManager.hasDeterminantsToRecoverFrom())
			 return  causalLoggingManager.getRecoveryRNGDeterminant().getNumber();

		int generatedNumber = random.nextInt(maxExclusive);
		causalLoggingManager.appendDeterminant(new RNGDeterminant(generatedNumber));
		return generatedNumber;
	}

}
