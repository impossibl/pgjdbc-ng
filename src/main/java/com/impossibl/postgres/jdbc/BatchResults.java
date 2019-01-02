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


import static com.impossibl.postgres.jdbc.ErrorUtils.makeSQLException;

import java.sql.BatchUpdateException;
import java.util.Arrays;

import static java.lang.Long.min;
import static java.sql.Statement.EXECUTE_FAILED;
import static java.sql.Statement.SUCCESS_NO_INFO;
import static java.util.Arrays.fill;

/**
 * Utility for managing the "count" results of a
 * batch execution.

 */
interface BatchResults {

  void setBatchSize(int size);
  void setUpdateCount(int batchIdx, long count);
  BatchUpdateException getException(int batchIdx, String message, Exception cause);

}

class IntegerBatchResults implements BatchResults {

  int[] counts = new int[0];

  @Override
  public void setBatchSize(int size) {
    counts = new int[size];
    fill(counts, SUCCESS_NO_INFO);
  }

  @Override
  public void setUpdateCount(int batchIdx, long count) {
    counts[batchIdx] = (int) min(count, Integer.MAX_VALUE);
  }

  @Override
  public BatchUpdateException getException(int batchIdx, String message, Exception cause) {
    int[] counts = Arrays.copyOf(this.counts, batchIdx + 1);
    counts[batchIdx] = EXECUTE_FAILED;
    return new BatchUpdateException(message, null, 0, counts, cause != null ? makeSQLException(cause) : null);
  }

}

class LongBatchResults implements BatchResults {

  long[] counts = new long[0];

  @Override
  public void setBatchSize(int size) {
    counts = new long[size];
    fill(counts, SUCCESS_NO_INFO);
  }

  @Override
  public void setUpdateCount(int batchIdx, long count) {
    counts[batchIdx] = count;
  }

  @Override
  public BatchUpdateException getException(int batchIdx, String message, Exception cause) {
    long[] counts = Arrays.copyOf(this.counts, batchIdx + 1);
    counts[batchIdx] = EXECUTE_FAILED;
    return new BatchUpdateException(message, null, 0, counts, cause != null ? makeSQLException(cause) : null);
  }

}
