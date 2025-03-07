package usjpin.flink;

import lombok.*;

import java.util.Map;
import java.util.HashMap;

@Data
@SuperBuilder
public class JoinerConfig {
    @Builder.Default
    private Map<String, StreamConfig<?>> streamConfigs = new HashMap<>();
    private long stateRetentionMs;

    public static class JoinerConfigBuilder<C extends JoinerConfig, B extends JoinerConfigBuilder<C, B>> extends JoinerConfigBuilderImpl<C, B> {
        public <T> B addStream(StreamConfig<T> streamConfig) {
            if (this.streamConfigs$value == null) {
                this.streamConfigs$value = new HashMap<>();
            }
            this.streamConfigs$value.put(streamConfig.getName(), streamConfig);
            return self();
        }

        public B withStateRetention(long stateRetentionMs) {
            this.stateRetentionMs(stateRetentionMs);
            return self();
        }
    }
}