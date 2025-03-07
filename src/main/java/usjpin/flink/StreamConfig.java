package usjpin.flink;

import jdk.nashorn.internal.objects.annotations.Constructor;
import lombok.*;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.function.Function;

@Data
@Builder
public class StreamConfig<T> {
    private String name;
    private DataStream<T> stream;
    private Class<T> classType;
    private Function<T, String> partitionKeyExtractor;
    private Function<T, String> joinKeyExtractor;

    public StreamConfig(String name, DataStream<T> stream, Class<T> classType, Function<T, String> partitionKeyExtractor, Function<T, String> joinKeyExtractor) {
        this.name = name;
        this.stream = stream;
        this.classType = classType;
        this.partitionKeyExtractor = partitionKeyExtractor;
        this.joinKeyExtractor = joinKeyExtractor;
    }

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
            return new StreamConfig<T>(this.name, this.stream, this.classType, this.partitionKeyExtractor, this.joinKeyExtractor);
        }
    }
}