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

import java.util.*;

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
				.joinLogic(new IntegerSumJoinLogic())
				.build();

		DataStream<Integer> joinedStream = NWayJoiner.create(joinerConfig);

		joinedStream.print();
		
		// Execute program, beginning computation.
		env.execute("IntegerJoiner Example");
	}

	/**
	 * Example join logic that sums integers from three streams.
	 * Updated to use the NWayJoinerContext interface.
	 */
	static class IntegerSumJoinLogic implements JoinLogic<Integer> {
		@Override
		public void apply(NWayJoinerContext<Integer> context) {
			JoinerState state = context.getState();
			Map<String, NavigableSet<JoinableEvent<?>>> stateMap = state.getState();
			
			// Check if we have events from all three streams
			if (!stateMap.containsKey("stream1") || !stateMap.containsKey("stream2") || !stateMap.containsKey("stream3")) {
				return;
			}
			
			NavigableSet<JoinableEvent<?>> stream1Events = stateMap.get("stream1");
			NavigableSet<JoinableEvent<?>> stream2Events = stateMap.get("stream2");
			NavigableSet<JoinableEvent<?>> stream3Events = stateMap.get("stream3");
			
			if (stream1Events.isEmpty() || stream2Events.isEmpty() || stream3Events.isEmpty()) {
				return;
			}
			
			// Get one event from each stream
			JoinableEvent<?> event1 = stream1Events.first();
			JoinableEvent<?> event2 = stream2Events.first();
			JoinableEvent<?> event3 = stream3Events.first();
			
			// Extract the integer values
			Integer value1 = (Integer) event1.getEvent();
			Integer value2 = (Integer) event2.getEvent();
			Integer value3 = (Integer) event3.getEvent();
			
			// Calculate the sum
			Integer sum = value1 + value2 + value3;
			
			// Emit the result
			context.emit(sum);
			
			// Remove the processed events
			stream1Events.remove(event1);
			stream2Events.remove(event2);
			stream3Events.remove(event3);
			
			// Clean up empty sets
			if (stream1Events.isEmpty()) stateMap.remove("stream1");
			if (stream2Events.isEmpty()) stateMap.remove("stream2");
			if (stream3Events.isEmpty()) stateMap.remove("stream3");
			
			// Update the state
			context.updateState(state);
		}
	}

	/**
	 * Source that generates random integers.
	 */
	public static class InfiniteSource implements SourceFunction<Integer> {
		private boolean running = true;
		private final Random random;
		private final int maxValue;

		public InfiniteSource(int maxValue) {
			this.maxValue = maxValue;
			this.random = new Random();
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (running) {
				ctx.collect(random.nextInt(maxValue));
				Thread.sleep(1000);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
