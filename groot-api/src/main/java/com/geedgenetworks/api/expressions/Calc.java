package com.geedgenetworks.api.expressions;

import java.io.Serializable;
import java.util.Map;

/**
 * The Calc interface represents a calculation operation that can be evaluated with input data
 * provided as a map.
 *
 * @author chaoc
 * @since 1.0
 */
public interface Calc extends Serializable {

    /**
     * Evaluates the calculation operation with the given input data.
     *
     * @param data A map containing input data for the calculation.
     * @return The result of the calculation operation.
     */
    Object eval(final Map<String, Object> data);
    Boolean filter(final Map<String, Object> data);

}
