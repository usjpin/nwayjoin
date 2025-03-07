package usjpin.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


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
                    .timestamp(context.timestamp())
                    .build()
        );
    }

    public static <T> DataStream<JoinableEvent<T>> format(StreamConfig<T> streamConfig) {
        return streamConfig.getStream()
                .keyBy(streamConfig.getPartitionKeyExtractor()::apply)
                .process(new EventFormatter<>(streamConfig));
    }
}
