package usjpin.flink;

import lombok.*;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.function.Function;

@Data
@Builder
public class StreamConfig<T> {
    private String name;
    private Function<T, String> partitionKeyExtractor;
    private Function<T, String> joinKeyExtractor;
    private DataStream<T> stream;
    private Class<T> classType;

    public static class Builder<T> {
        private String name;
        private Function<T, String> partitionKeyExtractor;
        private Function<T, String> joinKeyExtractor;
        private DataStream<T> stream;
        private Class<T> classType;

        public Builder<T> withStream(String name, DataStream<T> stream, Class<T> classType) {
            this.name = name;
            this.stream = stream;
            this.classType = classType;
            return this;
        }

        public Builder<T> withPartitionKeyExtractor(Function<T, String> partitionKeyExtractor) {
            this.partitionKeyExtractor = partitionKeyExtractor;
            return this;
        }

        public Builder<T> withJoinKeyExtractor(Function<T, String> joinKeyExtractor) {
            this.joinKeyExtractor = joinKeyExtractor;
            return this;
        }

        public StreamConfig<T> build() {
            return new StreamConfig<T>(this.name, this.partitionKeyExtractor, this.joinKeyExtractor, this.stream, this.classType);
        }
    }
}