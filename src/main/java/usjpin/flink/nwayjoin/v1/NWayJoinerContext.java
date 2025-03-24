package usjpin.flink.nwayjoin.v1;

import java.io.Serializable;

/**
 * Rich context interface for NWayJoiner that provides access to state and output operations.
 * This allows join implementations to have more control over state management and output.
 *
 * @param <OUT> The output type of the join operation
 */
public interface NWayJoinerContext<OUT> extends Serializable {
    /**
     * Updates the joiner state.
     *
     * @param state The updated state
     */
    void updateState(JoinerState state);
    
    /**
     * Emits a result to the output.
     *
     * @param result The result to emit
     */
    void emit(OUT result);
    
    /**
     * Gets the current joiner state.
     *
     * @return The current state
     */
    JoinerState getState();
    
    /**
     * Gets the current processing timestamp.
     *
     * @return The current timestamp in milliseconds
     */
    long getCurrentTimestamp();
} 