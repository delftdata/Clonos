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

import java.io.Serializable;
import java.util.function.Predicate;

public class InFlightLogConfig implements Serializable {


	public static final ConfigOption<String> IN_FLIGHT_LOG_TYPE  = ConfigOptions
		.key("taskmanager.inflight.type")
		.defaultValue("spillable")
		.withDescription("The type of inflight log to use. \"inmemory\" for a fully in memory one, \"spillable\" " +
			"for one that is spilled to disk asynchronously");


	public static final ConfigOption<String> IN_FLIGHT_LOG_SPILL_POLICY = ConfigOptions
		.key("taskmanager.inflight.spill.policy")
		.defaultValue("eager")
		.withDescription("The policy to use for when to spill the in-flight log. \"eager\" for one that spills on " +
			"write, \"availability\" for one that spills at a given buffer availability level, \"epoch\" for one that" +
			" spills on every epoch completion.");

	public static final ConfigOption<Float> AVAILABILITY_POLICY_FILL_FACTOR = ConfigOptions
		.key("taskmanager.inflight.spill.availability-trigger")
		.defaultValue(0.3f)
		.withDescription("The availability level at and under which a flush of the inflight log is triggered.");

	private final Configuration config;


	private Float availabilityPolicyFillFactor;

	public enum Type {
		IN_MEMORY, SPILLABLE
	}


	public InFlightLogConfig(Configuration config){
		this.config = config;
	}

	public Type getType() {
		String type = config.getString(IN_FLIGHT_LOG_TYPE);

		switch (type){
			case "inmemory":
				return Type.IN_MEMORY;
			case "spillable":
				return Type.SPILLABLE;
			default:
				return Type.SPILLABLE;
		}
	}


	public Predicate<SpillableSubpartitionInFlightLogger> getSpillPolicy() {
		String policy = config.getString(IN_FLIGHT_LOG_SPILL_POLICY);

		switch (policy){
			case "eager":
				return eagerPolicy;
			case "epoch":
				return epochPolicy;
			case "availability":
			default:
				return availabilityPolicy;
		}
	}

	public float getAvailabilityPolicyFillFactor(){
		if(availabilityPolicyFillFactor == null)
			availabilityPolicyFillFactor = config.getFloat(AVAILABILITY_POLICY_FILL_FACTOR);

		return availabilityPolicyFillFactor;
	}

	public static Predicate<SpillableSubpartitionInFlightLogger> eagerPolicy = log -> true;

	public static Predicate<SpillableSubpartitionInFlightLogger> availabilityPolicy = SpillableSubpartitionInFlightLogger::isPoolAvailabilityLow;

	public static Predicate<SpillableSubpartitionInFlightLogger> epochPolicy = SpillableSubpartitionInFlightLogger::hasFullUnspilledEpochUnsafe;

}
