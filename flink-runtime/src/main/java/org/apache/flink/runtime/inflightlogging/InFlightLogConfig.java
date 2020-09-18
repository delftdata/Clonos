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

package org.apache.flink.runtime.inflightlogging;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public class InFlightLogConfig implements Serializable {


	public static final ConfigOption<String> IN_FLIGHT_LOG_TYPE = ConfigOptions
		.key("taskmanager.inflight.type")
		.defaultValue("spillable")
		.withDescription("The type of inflight log to use. \"inmemory\" for a fully in memory one, \"spillable\" " +
			"for one that is spilled to disk asynchronously");


	public static final ConfigOption<String> IN_FLIGHT_LOG_SPILL_POLICY = ConfigOptions
		.key("taskmanager.inflight.spill.policy")
		.defaultValue("eager")
		.withDescription("The policy to use for when to spill the in-flight log. \"eager\" for one that spills on " +
			"write, \"availability\" for one that spills at a given buffer availability level, \"epoch\" for one " +
			"that" +
			" spills on every epoch completion.");

	public static final ConfigOption<Integer> IN_FLIGHT_LOG_SPILL_NUM_RECOVERY_BUFFERS = ConfigOptions
		.key("taskmanager.inflight.spill.num-recovery-buffers")
		.defaultValue(50)
		.withDescription("The number of buffers each pipelined subpartition reserves for reading spilled buffers and " +
			"sending downstream");

	public static final ConfigOption<Long> IN_FLIGHT_LOG_SPILL_SLEEP = ConfigOptions
		.key("taskmanager.inflight.spill.sleep")
		.defaultValue(50L)
		.withDescription("How long to sleep between tests of the policy");

	public static final ConfigOption<Float> AVAILABILITY_POLICY_FILL_FACTOR = ConfigOptions
		.key("taskmanager.inflight.spill.availability-trigger")
		.defaultValue(0.3f)
		.withDescription("The availability level at and under which a flush of the inflight log is triggered.");


	private final Configuration config;


	public enum Type {
		IN_MEMORY, SPILLABLE
	}

	public enum Policy {
		EAGER, AVAILABILITY
	}


	public InFlightLogConfig(Configuration config) {
		this.config = config;
	}

	public Type getType() {
		String type = config.getString(IN_FLIGHT_LOG_TYPE);

		switch (type) {
			case "inmemory":
				return Type.IN_MEMORY;
			case "spillable":
			default:
				return Type.SPILLABLE;
		}
	}


	public Policy getSpillPolicy() {
		String policy = config.getString(IN_FLIGHT_LOG_SPILL_POLICY);

		switch (policy) {
			case "eager":
				return Policy.EAGER;
			case "availability":
				return Policy.AVAILABILITY;
			default:
				throw new RuntimeException("Requested synchronous spill policy for asynchronous global policy");
		}
	}

	public int getNumberOfRecoveryBuffers() {
		return config.getInteger(IN_FLIGHT_LOG_SPILL_NUM_RECOVERY_BUFFERS);
	}

	public float getAvailabilityPolicyFillFactor() {
		return config.getFloat(AVAILABILITY_POLICY_FILL_FACTOR);
	}

	public long getInFlightLogSleepTime() {
		return config.getLong(IN_FLIGHT_LOG_SPILL_SLEEP);
	}



	@Override
	public String toString() {
		return "InFlightLogConfig{"
			+ "type: " + getType()
			+ ", policy: " +config.getString(IN_FLIGHT_LOG_SPILL_POLICY)
			+ ", fill-factor: " + getAvailabilityPolicyFillFactor()
			+ "}";
	}
}
