package usjpin.flink.nwayjoin.v1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class NWayJoiner<OUT> extends KeyedProcessFunction<String, JoinableEvent<?>, OUT> {
    private final JoinerConfig<OUT> joinerConfig;
    private transient ValueState<JoinerState> joinState;
    private transient ValueState<Long> lastCleanupTimestamp;
    private transient ContextImpl context;

    public NWayJoiner(JoinerConfig<OUT> joinerConfig) {
        this.joinerConfig = joinerConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<JoinerState> joinStateDescriptor =
                new ValueStateDescriptor<>(
                        "joinState",
                        TypeInformation.of(new TypeHint<JoinerState>() {})
                );

        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Duration.ofMillis(joinerConfig.getStateRetentionMs()))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        joinStateDescriptor.enableTimeToLive(stateTtlConfig);

        joinState = getRuntimeContext().getState(joinStateDescriptor);

        lastCleanupTimestamp = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastCleanupTimestamp", Types.LONG));
                
        context = new ContextImpl();
    }

    @Override
    public void processElement(
            JoinableEvent<?> joinableEvent,
            KeyedProcessFunction<String, JoinableEvent<?>, OUT>.Context context,
            Collector<OUT> collector) throws Exception {
        if (lastCleanupTimestamp.value() == null) {
            lastCleanupTimestamp.update(0L);
        }
    
        if (joinState.value() == null) {
            joinState.update(new JoinerState());
        }

        JoinerState state = joinState.value();
        state.addEvent(joinableEvent);
        joinState.update(state);

        if (joinerConfig.getJoinTimeoutMs() == 0) {
            applyJoinLogic(context.timerService().currentProcessingTime(), collector);
        } else {
            context.timerService().registerProcessingTimeTimer(
                context.timerService().currentProcessingTime() + joinerConfig.getJoinTimeoutMs()
            );
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
        applyJoinLogic(timestamp, out);
    }

    private void applyJoinLogic(long timestamp, Collector<OUT> collector) throws Exception {
        JoinerState state = joinState.value();
        state.removeEventsOlderThan(timestamp - joinerConfig.getStateRetentionMs());
        
        // Set up the context with current state and timestamp
        this.context.setup(state, timestamp, collector);
        
        // Apply the join logic with the context
        joinerConfig.getJoinLogic().apply(this.context);
        
        // Update state if it was modified by the join logic
        if (this.context.isStateModified()) {
            joinState.update(this.context.getState());
        }

        // Use the configurable cleanup interval
        if (timestamp > lastCleanupTimestamp.value() + joinerConfig.getCleanupIntervalMs()) {
            state.removeEventsOlderThan(timestamp - joinerConfig.getStateRetentionMs());
            joinState.update(state);
            lastCleanupTimestamp.update(timestamp);
        }
    }

    public static <OUT> DataStream<OUT> create(JoinerConfig<OUT> joinerConfig) {
        DataStream<JoinableEvent<?>> formattedStreams = null;

        for (StreamConfig<?> config : joinerConfig.getStreamConfigs().values()) {
            DataStream<JoinableEvent<?>> currentStream = EventFormatter.format(config);
            if (formattedStreams == null) {
                formattedStreams = currentStream;
            } else {
                formattedStreams = formattedStreams.union(currentStream);
            }
        }
        
        if (formattedStreams == null) {
            throw new IllegalArgumentException("No streams to join");
        }

        return formattedStreams
                .keyBy(JoinableEvent::getJoinKey)
                .process(new NWayJoiner<>(joinerConfig))
                .returns(TypeInformation.of(joinerConfig.getOutClass()));
    }
    
    /**
     * Implementation of the NWayJoinerContext interface.
     */
    private class ContextImpl implements NWayJoinerContext<OUT> {
        private JoinerState state;
        private long timestamp;
        private Collector<OUT> collector;
        private boolean stateModified = false;
        
        public void setup(JoinerState state, long timestamp, Collector<OUT> collector) {
            this.state = state;
            this.timestamp = timestamp;
            this.collector = collector;
            this.stateModified = false;
        }
        
        @Override
        public void updateState(JoinerState state) {
            this.state = state;
            this.stateModified = true;
        }
        
        @Override
        public void emit(OUT result) {
            if (result != null) {
                collector.collect(result);
            }
        }
        
        @Override
        public JoinerState getState() {
            return state;
        }
        
        @Override
        public long getCurrentTimestamp() {
            return timestamp;
        }
        
        public boolean isStateModified() {
            return stateModified;
        }
    }
}
