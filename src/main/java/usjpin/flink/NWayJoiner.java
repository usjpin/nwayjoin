package usjpin.flink;

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
    private final long CLEANUP_INTERVAL_MS = 60_000L;

    private final JoinerConfig<OUT> joinerConfig;
    private transient ValueState<JoinerState> joinState;
    private transient ValueState<Long> lastCleanupTimestamp;

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
            attemptInnerJoin(context.timerService().currentProcessingTime(), collector);
        } else {
            context.timerService().registerProcessingTimeTimer(
                context.timerService().currentProcessingTime() + joinerConfig.getJoinTimeoutMs()
            );
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
        attemptInnerJoin(timestamp, out);
    }

    private void attemptInnerJoin(long timestamp, Collector<OUT> collector) throws Exception {
        JoinerState state = joinState.value();
        state.removeEventsOlderThan(timestamp - joinerConfig.getStateRetentionMs());
        OUT result = joinerConfig.getJoinLogic().apply(state);
        if (result == null) {
            return;
        }

        collector.collect(result);

        if (timestamp > lastCleanupTimestamp.value() + CLEANUP_INTERVAL_MS) {
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
}
