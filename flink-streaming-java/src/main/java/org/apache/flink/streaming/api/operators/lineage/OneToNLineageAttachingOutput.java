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
package org.apache.flink.streaming.api.operators.lineage;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.RecordID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneToNLineageAttachingOutput<OUT> extends AbstractLineageAttachingOutput<OUT> {


	private int counter;
	private RecordID inputBase;
	private RecordID outputResult;


	public OneToNLineageAttachingOutput(Output<StreamRecord<OUT>> outputToWrap) {
		super(outputToWrap);
		this.counter = 0;
		this.outputResult = new RecordID();
	}

	@Override
	public void notifyInputRecord(StreamRecord<?> input) {
		counter = 0;
		this.inputBase = input.getRecordID();
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		//skip
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		//skip
	}

	@Override
	protected RecordID getRecordIDForNextOutputRecord() {
		hashFunction.hashInt(inputBase.hashCode() + counter++).writeBytesTo(outputResult.getId(), 0, RecordID.NUMBER_OF_BYTES);
		return outputResult;
	}
}
