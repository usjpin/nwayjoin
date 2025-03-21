package usjpin.flink;

import lombok.*;

import java.io.Serializable;
import java.util.*;

@Data
@Builder
public class JoinerConfig<OUT> implements Serializable {
    private final Map<String, StreamConfig<?>> streamConfigs;
    private final Class<OUT> outClass;
    private final JoinLogic<OUT> joinLogic;
    private final long stateRetentionMs;
    private final long joinTimeoutMs;

    private JoinerConfig(
            Map<String, StreamConfig<?>> streamConfigs,
            Class<OUT> outClass,
            JoinLogic<OUT> joinLogic,
            long stateRetentionMs,
            long joinTimeoutMs) {
        this.streamConfigs = streamConfigs;
        this.outClass = outClass;
        this.joinLogic = joinLogic;
        this.stateRetentionMs = stateRetentionMs;
        this.joinTimeoutMs = joinTimeoutMs;
    }

    public static <OUT> Builder<OUT> builder() {
        return new Builder<>();
    }

    public static class Builder<OUT> {
        private final Map<String, StreamConfig<?>> streamConfigs = new HashMap<>();
        private Class<OUT> outClass;
        private JoinLogic<OUT> joinLogic;
        private long stateRetentionMs = 60_000L;
        private long joinTimeoutMs = 0L;

        public Builder<OUT> addStreamConfig(StreamConfig<?> streamConfig) {
            this.streamConfigs.put(streamConfig.getName(), streamConfig);
            return this;
        }

        public Builder<OUT> outClass(Class<OUT> outClass) {
            this.outClass = outClass;
            return this;
        }

        public Builder<OUT> joinLogic(JoinLogic<OUT> joinLogic) {
            this.joinLogic = joinLogic;
            return this;
        }

        public Builder<OUT> stateRetentionMs(long stateRetentionMs) {
            if (stateRetentionMs <= 0) {
                throw new IllegalArgumentException("State retention must be positive");
            }
            this.stateRetentionMs = stateRetentionMs;
            return this;
        }

        public Builder<OUT> joinTimeoutMs(long joinTimeoutMs) {
            if (joinTimeoutMs < 0) {
                throw new IllegalArgumentException("Join timeout must be positive");
            }
            this.joinTimeoutMs = joinTimeoutMs;
            return this;
        }

        public JoinerConfig<OUT> build() {
            if (streamConfigs.isEmpty()) {
                throw new IllegalStateException("At least one stream config must be provided");
            }
            if (joinLogic == null) {
                throw new IllegalStateException("Join logic must be provided");
            }
            if (outClass == null) {
                throw new IllegalStateException("Output class must be provided");
            }
            return new JoinerConfig<>(streamConfigs, outClass, joinLogic, stateRetentionMs, joinTimeoutMs);
        }
    }
}