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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.RecordID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.Map;

public class DefaultSourceLineageAttachingOutput<K, OUT> extends AbstractLineageAttachingOutput<OUT> implements SourceLineageAttachingOutput<K, OUT> {


	private ListState<Tuple2<K, Integer>> managedNumRecordsEmitted;
	protected Map<K, Integer> numRecordsEmitted;

	private K currentKey;
	private Integer currentKeyHash;
	private Map<K, Integer> hashCodeCache;

	public DefaultSourceLineageAttachingOutput(Output<StreamRecord<OUT>> outputToWrap) {
		super(outputToWrap);
		this.numRecordsEmitted = new HashMap<>();
		this.hashCodeCache = new HashMap<>();
	}

	@Override
	public void setKey(K key) {
		this.currentKey = key;
		this.currentKeyHash = hashCodeCache.get(key);
		if (this.currentKeyHash == null) {
			this.currentKeyHash = currentKey.hashCode();
			hashCodeCache.put(currentKey, currentKeyHash);
		}

	}


	@Override
	protected RecordID getRecordIDForNextOutputRecord() {
		if (currentKey == null)
			throw new MissingLineageContextException("Forgot to call setKey() before getRecordIDForNextOutputRecord.");
		RecordID toReturn = new RecordID();
		Integer currentNumRecordsEmitted = this.numRecordsEmitted.get(currentKey);
		hashFunction.hashInt(this.currentKeyHash + currentNumRecordsEmitted).writeBytesTo(toReturn.getId(), 0, RecordID.NUMBER_OF_BYTES);
		currentNumRecordsEmitted++;
		return toReturn;
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		this.managedNumRecordsEmitted = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(LINEAGE_INFO_NAME, TypeInformation.of(new TypeHint<Tuple2<K, Integer>>() {
		})));

		if (context.isRestored())
			for (Tuple2<K, Integer> pair : managedNumRecordsEmitted.get())
				this.numRecordsEmitted.put(pair.f0, pair.f1);

	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		this.managedNumRecordsEmitted.clear();
		for (Map.Entry<K, Integer> entry : numRecordsEmitted.entrySet())
			this.managedNumRecordsEmitted.add(new Tuple2<>(entry.getKey(), entry.getValue()));

	}

	@Override
	public void notifyInputRecord(StreamRecord<?> input) {
		//skip, its a source, no input records
	}
}
