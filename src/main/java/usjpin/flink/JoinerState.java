package usjpin.flink;

import lombok.Getter;

import java.util.*;

@Getter
public class JoinerState {
  private final Map<String, SortedSet<JoinableEvent<?>>> state;

  public JoinerState() {
    this.state = new HashMap<>();
  }

  public void addEvent(JoinableEvent<?> event) {

    state.computeIfAbsent(event.getStreamName(), k -> new TreeSet<>()).add(event);
  }
  
  public void removeEventsOlderThan(long timestamp) {
    for (Set<JoinableEvent<?>> events : state.values()) {
      events.removeIf(event -> event.getTimestamp() < timestamp);
    }
    state.entrySet().removeIf(entry -> entry.getValue().isEmpty());
  }
}
