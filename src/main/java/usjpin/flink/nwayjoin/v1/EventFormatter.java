package usjpin.flink.nwayjoin.v1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.Types;

public class EventFormatter<T> extends KeyedProcessFunction<String, T, JoinableEvent<T>> {
    private final StreamConfig<T> streamConfig;

    public EventFormatter(StreamConfig<T> streamConfig) {
        this.streamConfig = streamConfig;
    }

    @Override
    public void processElement(T event, KeyedProcessFunction<String, T, JoinableEvent<T>>.Context context, Collector<JoinableEvent<T>> collector) {
        collector.collect(
            JoinableEvent.<T>builder()
                    .streamName(streamConfig.getName())
                    .event(event)
                    .joinKey(streamConfig.getJoinKeyExtractor().apply(event))
                    .timestamp(context.timerService().currentProcessingTime())
                    .build()
        );
    }

    private static class WildcardMapFunction<T> implements MapFunction<JoinableEvent<T>, JoinableEvent<?>>, ResultTypeQueryable<JoinableEvent<?>> {
        @Override
        public JoinableEvent<?> map(JoinableEvent<T> event) {
            return event;
        }

        @Override
        public TypeInformation<JoinableEvent<?>> getProducedType() {
            return TypeInformation.of(new TypeHint<JoinableEvent<?>>() {});
        }
    }

    public static <T> DataStream<JoinableEvent<?>> format(StreamConfig<T> streamConfig) {
        return streamConfig.getStream()
                .keyBy((KeySelector<T, String>) value -> streamConfig.getJoinKeyExtractor().apply(value), Types.STRING)
                .process(new EventFormatter<>(streamConfig))
                .map(new WildcardMapFunction<>());
    }
}
