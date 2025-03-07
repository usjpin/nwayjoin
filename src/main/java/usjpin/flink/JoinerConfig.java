package usjpin.flink;

import lombok.*;

import java.util.HashMap;
import java.util.Map;

@Data
@Builder
public class JoinerConfig {
    private Map<String, StreamConfig<?>> streamConfigs;
    private long stateRetentionMs;

    public JoinerConfig(Map<String, StreamConfig<?>> streamConfigs, long stateRetentionMs) {
        this.streamConfigs = streamConfigs;
        this.stateRetentionMs = stateRetentionMs;
    }

    public static class Builder {
        private long stateRetentionMs;
        private final Map<String, StreamConfig<?>> streamConfigs = new HashMap<>();

        public <T> Builder addStream(StreamConfig<T> streamConfig) {
            this.streamConfigs.put(streamConfig.getName(), streamConfig);
            return this;
        }

        public Builder withStateRetention(long stateRetentionMs) {
            this.stateRetentionMs = stateRetentionMs;
            return this;
        }

        public JoinerConfig build() {
            return new JoinerConfig(this.streamConfigs, this.stateRetentionMs);
        }
    }
}