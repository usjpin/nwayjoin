package usjpin.flink.nwayjoin.v1;

import lombok.Getter;

import java.io.Serializable;
import java.util.*;

/**
 * State container for the NWayJoiner.
 * Maintains events from different streams, sorted by timestamp.
 */
@Getter
public class JoinerState implements Serializable {
  // Main state storage - stream name to sorted events (already sorted by timestamp)
  private final Map<String, NavigableSet<JoinableEvent<?>>> state;

  public JoinerState() {
    this.state = new HashMap<>();
  }

  /**
   * Adds an event to the state.
   */
  public void addEvent(JoinableEvent<?> event) {
    state.computeIfAbsent(event.getStreamName(), k -> new TreeSet<>()).add(event);
  }
  
  /**
   * Removes events older than the specified timestamp.
   */
  public void removeEventsOlderThan(long timestamp) {
    for (Set<JoinableEvent<?>> events : state.values()) {
      events.removeIf(event -> event.getTimestamp() < timestamp);
    }
    state.entrySet().removeIf(entry -> entry.getValue().isEmpty());
  }
  
  /**
   * Gets the most recent event from a stream before a given timestamp.
   * Useful for attribution to most recent click.
   */
  public JoinableEvent<?> getMostRecentEventBefore(String streamName, long timestamp) {
    NavigableSet<JoinableEvent<?>> events = state.get(streamName);
    if (events == null || events.isEmpty()) {
      return null;
    }
    
    // Find the event with the highest timestamp that's less than the given timestamp
    for (JoinableEvent<?> event : events.descendingSet()) {
      if (event.getTimestamp() < timestamp) {
        return event;
      }
    }
    
    return null;
  }
  
  /**
   * Gets events from a specific stream within a time range.
   * Useful for finding events in a session window.
   */
  public List<JoinableEvent<?>> getEventsInTimeRange(String streamName, long startTime, long endTime) {
    NavigableSet<JoinableEvent<?>> events = state.get(streamName);
    if (events == null || events.isEmpty()) {
      return Collections.emptyList();
    }
    
    List<JoinableEvent<?>> result = new ArrayList<>();
    for (JoinableEvent<?> event : events) {
      if (event.getTimestamp() >= startTime && event.getTimestamp() <= endTime) {
        result.add(event);
      } else if (event.getTimestamp() > endTime) {
        // Since events are sorted by timestamp, we can break early
        break;
      }
    }
    
    return result;
  }
  
  /**
   * Removes a specific event from the state.
   */
  public void removeEvent(JoinableEvent<?> event) {
    NavigableSet<JoinableEvent<?>> events = state.get(event.getStreamName());
    if (events != null) {
      events.remove(event);
      if (events.isEmpty()) {
        state.remove(event.getStreamName());
      }
    }
  }
}
