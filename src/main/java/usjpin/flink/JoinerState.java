package usjpin.flink;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class JoinerState {
  private final Map<String, Set<JoinableEvent<?>>> state;

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
