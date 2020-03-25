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
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.RecordID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * One to one operators (Map, Filter, ...) may simply store the input record's {@link RecordID} and forward it.
 *
 * @param <OUT>
 */
public class OneToOneLineageAttachingOutput<OUT> extends AbstractLineageAttachingOutput<OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(OneToOneLineageAttachingOutput.class);

	private RecordID toOutput;

	public OneToOneLineageAttachingOutput(Output<StreamRecord<OUT>> outputToWrap) {
		super(outputToWrap);
	}

	@Override
	public void notifyInputRecord(StreamRecord<?> input) {
		LOG.debug("Received input record with ID: {}", input.getRecordID());
		//Avoid cloning as it wont be changed. Possibly dont even have to attach it, if the same StreamRecord instance is used.
		toOutput = input.getRecordID();
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
		return toOutput;
	}
}
