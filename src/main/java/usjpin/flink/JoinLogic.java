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
     * Applies the join logic using the provided context.
     * The implementation can access state, emit results, and get timestamps through the context.
     *
     * @param context The joiner context providing access to state and output operations
     */
    void apply(NWayJoinerContext<OUT> context);
}