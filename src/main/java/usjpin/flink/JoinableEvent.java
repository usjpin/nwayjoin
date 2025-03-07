package usjpin.flink;

import lombok.*;

@Data
@Builder
public class JoinableEvent<T> {
  private final String streamName;
  private final T event;
  private final String joinKey;
  private final long timestamp;
}

