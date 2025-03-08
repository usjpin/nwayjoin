/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package usjpin.flink.examples;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.function.SerializableFunction;
import usjpin.flink.*;

import java.util.Map;
import java.util.Random;
import java.util.SortedSet;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class IntegerJoiner {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Integer> dataStream1 = env.addSource(new InfiniteSource(100));
		DataStream<Integer> dataStream2 = env.addSource(new InfiniteSource(100));
		DataStream<Integer> dataStream3 = env.addSource(new InfiniteSource(100));

		SerializableFunction<Integer, String> joinKeyExtractor = Object::toString;

		StreamConfig<Integer> streamConfig1 = StreamConfig.<Integer>builder()
				.withStream("stream1", dataStream1, Integer.class)
				.joinKeyExtractor(joinKeyExtractor)
				.build();

		StreamConfig<Integer> streamConfig2 = StreamConfig.<Integer>builder()
				.withStream("stream2", dataStream2, Integer.class)
				.joinKeyExtractor(joinKeyExtractor)
				.build();

		StreamConfig<Integer> streamConfig3 = StreamConfig.<Integer>builder()
				.withStream("stream3", dataStream3, Integer.class)
				.joinKeyExtractor(joinKeyExtractor)
				.build();

		JoinerConfig<Integer> joinerConfig = JoinerConfig.<Integer>builder()
				.addStreamConfig(streamConfig1)
				.addStreamConfig(streamConfig2)
				.addStreamConfig(streamConfig3)
				.outClass(Integer.class)
				.stateRetentionMs(3600*1000L)
				.joinLogic(
						joinerState -> {
							Map<String, SortedSet<JoinableEvent<?>>> state = joinerState.getState();

							if (state.size() != 3) {
								return null;
							}

							Integer stream1Val = (Integer)state.get("stream1").iterator().next().getEvent();
							Integer stream2Val = (Integer)state.get("stream2").iterator().next().getEvent();
							Integer stream3Val = (Integer)state.get("stream3").iterator().next().getEvent();

							return stream1Val + stream2Val + stream3Val;
						}
				)
				.build();

		DataStream<Integer> joinedStream = NWayJoiner.create(joinerConfig);

		joinedStream.print();
		
		// Execute program, beginning computation.
		env.execute("IntegerJoiner Example");
	}

	public static class InfiniteSource implements SourceFunction<Integer> {
		private final Random random;
		private final int maxValue;
		private boolean isRunning = true;

		public InfiniteSource(int maxValue) {
			this.maxValue = maxValue;
			this.random = new Random();
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (isRunning) {
				ctx.collect(random.nextInt(maxValue));
				Thread.sleep(1000);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}
}
