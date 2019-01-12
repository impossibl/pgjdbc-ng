package com.impossibl.jdbc.spy;

/**
 * Common Relay interface that provides access
 * to its target object.
 */
public interface Relay<T> {

  T getTarget();

}
