package usjpin.flink;

import lombok.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@Data
@Builder
public class JoinerConfig<OUT> {
    private Map<String, StreamConfig<?>> streamConfigs;
    private Function<Map<String, List<JoinableEvent<?>>>, OUT> joinLogic;
    private long stateRetentionMs;

    public JoinerConfig(
            Map<String, StreamConfig<?>> streamConfigs,
            Function<Map<String, List<JoinableEvent<?>>>, OUT> joinLogic,
            long stateRetentionMs) {
        this.streamConfigs = streamConfigs;
        this.joinLogic = joinLogic;
        this.stateRetentionMs = stateRetentionMs;
    }

    public static class Builder<OUT> {
        private long stateRetentionMs;
        private Function<Map<String, List<JoinableEvent<?>>>, OUT> joinLogic;
        private final Map<String, StreamConfig<?>> streamConfigs = new HashMap<>();

        public <T> Builder<OUT> addStream(StreamConfig<T> streamConfig) {
            this.streamConfigs.put(streamConfig.getName(), streamConfig);
            return this;
        }

        public Builder<OUT> withStateRetention(long stateRetentionMs) {
            this.stateRetentionMs = stateRetentionMs;
            return this;
        }

        public Builder<OUT> withJoinLogic(Function<Map<String, List<JoinableEvent<?>>>, OUT> joinLogic) {
            this.joinLogic = joinLogic;
            return this;
        }

        public JoinerConfig<OUT> build() {
            return new JoinerConfig<OUT>(this.streamConfigs, this.joinLogic, this.stateRetentionMs);
        }
    }
}