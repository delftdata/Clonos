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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.RecordID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @param <K>
 * @param <W>
 * @param <OUT>
 */
public class NonMergingWindowLineageAttachingOutput<K, W, OUT> extends AbstractWindowLineageAttachingOutput<K, W, OUT> {


	private static final Logger LOG = LoggerFactory.getLogger(NonMergingWindowLineageAttachingOutput.class);

	public NonMergingWindowLineageAttachingOutput(Output<StreamRecord<OUT>> outputToWrap) {
		super(outputToWrap);
	}


	@Override
	public void notifyInputRecord(StreamRecord<?> input) {
		latestInserted = input.getRecordID();
	}


	@Override
	protected RecordID getRecordIDForNextOutputRecord() {
		Tuple2<RecordID, Integer> current = this.paneToRecordIDReduction.get(currentContext);

		RecordID toReturn = new RecordID();
		if(current == null)//No record has been assigned to window.
			hashFunction.hashInt(currentContext.hashCode()).writeBytesTo(toReturn.getId(), 0, RecordID.NUMBER_OF_BYTES);
		else {
			hashFunction.hashInt(currentContext.hashCode() + current.f0.hashCode() + current.f1).writeBytesTo(toReturn.getId(), 0, RecordID.NUMBER_OF_BYTES);
			current.f1++; //Increase the counter.
		}
		return toReturn;
	}

}
