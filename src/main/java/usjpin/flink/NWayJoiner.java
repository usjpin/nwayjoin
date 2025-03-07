package usjpin.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

class NWayJoiner<OUT> extends KeyedProcessFunction<String, JoinableEvent<?>, OUT> {

    public NWayJoiner(JoinerConfig joinerConfig) {

    }

    @Override
    public void processElement(
            JoinableEvent<?> joinableEvent,
            KeyedProcessFunction<String, JoinableEvent<?>, OUT>.Context context,
            Collector<OUT> collector) throws Exception {

    }

    public static <OUT> DataStream<OUT> create(JoinerConfig joinerConfig) {
        DataStream<JoinableEvent<?>> formattedStreams = null;

        for (StreamConfig<?> config : joinerConfig.getStreamConfigs().values()) {
            DataStream<JoinableEvent<?>> currentStream = EventFormatter.format(config)
                    .map(event -> (JoinableEvent<?>) event);
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
                .process(new NWayJoiner<>(joinerConfig));
    }
}