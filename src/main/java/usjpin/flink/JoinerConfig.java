package usjpin.flink;

import lombok.*;

import java.util.*;
import java.util.function.Function;

@Data
@Builder
public class JoinerConfig<OUT> {
    private Map<String, StreamConfig<?>> streamConfigs;
    private Function<Map<String, List<JoinableEvent<?>>>, OUT> joinLogic;
    private long stateRetentionMs;
    private long joinTimeoutMs;

    public JoinerConfig(
            Map<String, StreamConfig<?>> streamConfigs,
            Function<Map<String, List<JoinableEvent<?>>>, OUT> joinLogic,
            long stateRetentionMs,
            long joinTimeoutMs) {
        this.streamConfigs = streamConfigs;
        this.joinLogic = joinLogic;
        this.stateRetentionMs = stateRetentionMs;
        this.joinTimeoutMs = joinTimeoutMs;
    }

    public static class Builder<OUT> {
        private long stateRetentionMs;
        private Function<Map<String, List<JoinableEvent<?>>>, OUT> joinLogic;
        private final Map<String, StreamConfig<?>> streamConfigs = new HashMap<>();
        private long joinTimeoutMs;

        public <T> Builder<OUT> addStream(StreamConfig<T> streamConfig) {
            this.streamConfigs.put(streamConfig.getName(), streamConfig);
            return this;
        }

        public Builder<OUT> withJoinLogic(Function<Map<String, List<JoinableEvent<?>>>, OUT> joinLogic) {
            this.joinLogic = joinLogic;
            return this;
        }

        public Builder<OUT> withStateRetention(long stateRetentionMs) {
            if (stateRetentionMs <= 0) {
                throw new IllegalArgumentException("State retention must be greater than 0");
            }
            this.stateRetentionMs = stateRetentionMs;
            return this;
        }

        public Builder<OUT> withJoinTimeout(long joinTimeoutMs) {
            if (joinTimeoutMs < 0) {
                throw new IllegalArgumentException("Join timeout must be >= 0");
            }
            this.joinTimeoutMs = joinTimeoutMs;
            return this;
        }

        public JoinerConfig<OUT> build() {
            return new JoinerConfig<OUT>(this.streamConfigs, this.joinLogic, this.stateRetentionMs, this.joinTimeoutMs);
        }
    }
}