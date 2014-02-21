/**
 * Copyright (c) 2013, impossibl.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of impossibl.com nor the names of its contributors may
 *    be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.impossibl.postgres.jdbc;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Last Recently Used PreparedStatement cache with eviction listener support
 * implementation.
 * 
 * @author Brett Wooldridge
 */
class LruStatementCache {

  /**
   * The maxSize of the cache.
   */
  private final int maxSize;

  /**
   * We use a LinkedHashMap with _access order_ specified in the constructor.
   * See LinkedHashMap documentation.
   */
  private final LinkedHashMap<CacheKey, PGPreparedStatement> cache;

  /**
   * A listener concerned with prepared statement cache evictions.
   */
  private final LruEvictionListener evictionListner;

  /**
   * See the LinkedHashMap documentation. We maintain our own size here, rather than
   * calling size(), because size() on a LinkedHashMap is proportional in time O(n)
   * with the size of the collection. Tracking size ourselves provides O(1) access.
   */
  private volatile int size;

  /**
   * A flag that is set during clear operations to prevent statements that are
   * closing from coming back into the cache.
   */
  private AtomicBoolean clearInProgress;

  public LruStatementCache(int maxSize, LruEvictionListener evictionListener) {
    this.maxSize = maxSize;
    this.cache = new LinkedHashMap<CacheKey, PGPreparedStatement>(maxSize + 1, 1.1f, true);
    this.evictionListner = evictionListener;
    this.clearInProgress = new AtomicBoolean();
  }

  /**
   * The provided key contains all pertinent information such as SQL statement,
   * auto-generated keys, cursor holdability, etc. It is a complete key for a
   * cached statement.  If the prepared statement exists in the cache it is
   * removed while in use, and returned later when the statement is closed.
   * 
   * @param key the calculated cache key
   * @return the cached PGPreparedStatement statement, or null
   */
  public PGPreparedStatement borrowStatement(CacheKey key) {
    PGPreparedStatement statement = cache.remove(key);
    if (statement != null) {
      --size;
      return statement;
    }

    return null;
  }

  /**
   * Return a statement to the cache, this is called when a statement is
   * closed.  If there is an existing cached statement for the same SQL,
   * the existing entry is evicted and replaced with this returning
   * statement (it's "fresher").
   * 
   * @param key a prepared statement calculated key
   * @return a prepared statement
   */
  public void returnStatement(CacheKey key, PGPreparedStatement statement) {
    if (clearInProgress.get() || maxSize < 1) {
      return;
    }

    PGPreparedStatement cached = cache.get(key);
    if (cached == null) {
      cache.put(key, statement);
      size++;
    }
    else {
      evictionListner.onEviction(cached);
      cache.put(key, statement);
    }

    // If the size is exceeded, we will evict one (or more)
    // statements until the max level is again reached.
    if (size > maxSize) {
      tryEviction();
    }
  }

  /**
   * Evict all statements from the cache. This likely happens on connection close.
   */
  protected void clear() {
    if (clearInProgress.compareAndSet(false, true)) {
      try {
        Iterator<Entry<CacheKey, PGPreparedStatement>> it = cache.entrySet().iterator();
        while (it.hasNext()) {
          Entry<CacheKey, PGPreparedStatement> entry = it.next();
          PGPreparedStatement statement = entry.getValue();
          it.remove();
          evictionListner.onEviction(statement);
        }
        cache.clear();
        size = 0;
      }
      finally {
        clearInProgress.set(false);
      }
    }
  }

  /**
   * Try to evict statements from the cache until the cache is reduced to maxSize.
   */
  private void tryEviction() {
    // Iteration order of the LinkedHashMap is from LRU to MRU
    Iterator<Entry<CacheKey, PGPreparedStatement>> it = cache.entrySet().iterator();
    while (it.hasNext() && size > maxSize) {
      Entry<CacheKey, PGPreparedStatement> entry = it.next();
      PGPreparedStatement statement = entry.getValue();
      it.remove();
      size--;
      evictionListner.onEviction(statement);
    }
  }

  /**
   * Eviction listener interface for {@link LruStatementCache}.
   */
  interface LruEvictionListener {
    void onEviction(PGPreparedStatement value);
  }

  static final class CacheKey {
    // All of these attributes must match a proposed statement before the
    // statement can be considered "the same" and delivered from the cache.
    private String sql;
    private int resultSetType;
    private int resultSetConcurrency;
    private int resultSetHoldability;
    private boolean autoGeneratedKeys;
    private String[] columnNames;

    public CacheKey(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability, String[] columnNames) {
      this.sql = sql;
      this.resultSetType = resultSetType;
      this.resultSetConcurrency = resultSetConcurrency;
      this.resultSetHoldability = resultSetHoldability;
      if (columnNames != null) {
        this.autoGeneratedKeys = true;
        this.columnNames = new String[columnNames.length];
        System.arraycopy(columnNames, 0, this.columnNames, 0, columnNames.length);
      }
    }

    /**
     * Overridden equals() that takes all PreparedStatement attributes into account.
     */
    public boolean equals(Object obj) {
      if (!(obj instanceof CacheKey)) {
        return false;
      }

      CacheKey otherKey = (CacheKey) obj;
      if (!sql.equals(otherKey.sql)) {
        return false;
      }
      else if (resultSetType != otherKey.resultSetType) {
        return false;
      }
      else if (resultSetConcurrency != otherKey.resultSetConcurrency) {
        return false;
      }
      else if (!Arrays.equals(columnNames, otherKey.columnNames)) {
        return false;
      }
      else if (autoGeneratedKeys != otherKey.autoGeneratedKeys) {
        return false;
      }
      else if (resultSetHoldability != otherKey.resultSetHoldability) {
        return false;
      }

      return true;
    }

    public int hashCode() {
      return sql != null ? sql.hashCode() : System.identityHashCode(this);
    }
  }
}
