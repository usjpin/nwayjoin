package usjpin.flink;

import lombok.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.function.SerializableFunction;

import java.io.Serializable;

@Data
@Builder
public class StreamConfig<T> implements Serializable {
    private String name;
    private transient DataStream<T> stream;
    private Class<T> classType;
    private SerializableFunction<T, String> joinKeyExtractor;

    private StreamConfig(String name, DataStream<T> stream, Class<T> classType, SerializableFunction<T, String> joinKeyExtractor) {
        this.name = name;
        this.stream = stream;
        this.classType = classType;
        this.joinKeyExtractor = joinKeyExtractor;
    }

    public static <OUT> StreamConfig.Builder<OUT> builder() {
        return new StreamConfig.Builder<>();
    }

    public static class Builder<OUT> {
        private String name;
        private DataStream<OUT> stream;
        private Class<OUT> classType;
        private SerializableFunction<OUT, String> joinKeyExtractor;

        public Builder<OUT> withStream(String name, DataStream<OUT> stream, Class<OUT> classType) {
            this.name = name;
            this.stream = stream;
            this.classType = classType;
            return this;
        }

        public Builder<OUT> joinKeyExtractor(SerializableFunction<OUT, String> joinKeyExtractor) {
            this.joinKeyExtractor = joinKeyExtractor;
            return this;
        }

        public StreamConfig<OUT> build() {
            return new StreamConfig<OUT>(this.name, this.stream, this.classType, this.joinKeyExtractor);
        }
    }
}