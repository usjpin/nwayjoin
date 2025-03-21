package usjpin.flink;

import java.io.Serializable;

/**
 * Interface for implementing custom join logic in the NWayJoiner.
 * Implementations must be serializable to work with Flink's distributed execution.
 *
 * @param <OUT> The output type of the join operation
 */
public interface JoinLogic<OUT> extends Serializable {
    
    /**
     * Applies the join logic to the current joiner state.
     *
     * @param state The current state containing events from all streams
     * @return The joined result, or null if no join should be emitted
     */
    OUT apply(JoinerState state);
}