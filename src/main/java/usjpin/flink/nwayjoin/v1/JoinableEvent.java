package usjpin.flink.nwayjoin.v1;

import lombok.*;

@Data
@Builder
public class JoinableEvent<T> implements Comparable<JoinableEvent<T>> {
  private final String streamName;
  private final T event;
  private final String joinKey;
  private final long timestamp;

  @Override
  public int compareTo(JoinableEvent<T> o) {
    return Long.compare(this.timestamp, o.timestamp);
  }
}