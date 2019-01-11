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
package com.impossibl.postgres.api.data;

import java.util.Arrays;



public class Range<T> {

  public static class Flags {

    byte value;

    private static final byte RANGE_EMPTY   = 0x01; /* range is empty */
    private static final byte RANGE_LB_INC  = 0x02; /* lower bound is inclusive */
    private static final byte RANGE_UB_INC  = 0x04; /* upper bound is inclusive */
    private static final byte RANGE_LB_INF  = 0x08; /* lower bound is -infinity */
    private static final byte RANGE_UB_INF  = 0x10; /* upper bound is +infinity */
    private static final byte RANGE_LB_NULL = 0x20; /* lower bound is null */
    private static final byte RANGE_UB_NULL = 0x40; /* upper bound is null */

    public Flags(byte value) {
      super();
      this.value = value;
    }

    public byte getValue() {
      return value;
    }

    public boolean isEmpty() {
      return (value & RANGE_EMPTY) != 0;
    }

    public boolean hasLowerBound() {
      return (value & (RANGE_EMPTY | RANGE_LB_NULL | RANGE_LB_INF)) == 0;
    }

    public boolean isLowerBoundInclusive() {
      return (value & RANGE_LB_INC) != 0;
    }

    public boolean isLowerBoundInfinity() {
      return (value & RANGE_LB_INF) != 0;
    }

    public boolean hasUpperBound() {
      return (value & (RANGE_EMPTY | RANGE_UB_NULL | RANGE_UB_INF)) == 0;
    }

    public boolean isUpperBoundInclusive() {
      return (value & RANGE_UB_INC) != 0;
    }

    public boolean isUpperBoundInfinity() {
      return (value & RANGE_UB_INF) != 0;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + value;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Flags other = (Flags) obj;
      if (value != other.value)
        return false;
      return true;
    }

  }

  Flags flags;
  Object[] values;

  public static <U> Range<U> create(U lower, boolean lowerInc, U upper, boolean upperInc) {
    int flags = 0;
    flags |= lower == null ? Flags.RANGE_LB_NULL : 0;
    flags |= lowerInc ? Flags.RANGE_LB_INC : 0;
    flags |= lowerInc && lower == null ? Flags.RANGE_LB_INF : 0;
    flags |= upper == null ? Flags.RANGE_UB_NULL : 0;
    flags |= upperInc ? Flags.RANGE_UB_INC : 0;
    flags |= upperInc && lower == null ? Flags.RANGE_UB_INF : 0;
    flags |= lower == null && upper == null && !lowerInc && !upperInc ? Flags.RANGE_EMPTY : 0;
    return new Range<>(new Flags((byte) flags), new Object[] {lower, upper});
  }

  public static Range<?> createEmpty() {
    return new Range<>(new Flags((byte) (Flags.RANGE_EMPTY | Flags.RANGE_LB_NULL | Flags.RANGE_UB_NULL)), new Object[] {});
  }

  public Range(Flags flags, Object[] values) {
    this.flags = flags;
    this.values = values.clone();
  }

  public Flags getFlags() {
    return flags;
  }

  public boolean isEmpty() {
    return flags.isEmpty();
  }

  public boolean hasLowerBound() {
    return flags.hasLowerBound();
  }

  @SuppressWarnings("unchecked")
  public T getLowerBound() {
    return (T) values[0];
  }

  public void setLowerBound(T val) {
    values[0] = val;
    if (val == null) {
      flags.value |= Flags.RANGE_LB_NULL;
      if (!hasUpperBound())
        flags.value |= Flags.RANGE_EMPTY;
    }
  }

  public boolean isLowerBoundInfinity() {
    return flags.isLowerBoundInfinity();
  }

  public boolean isLowerBoundInclusive() {
    return flags.isLowerBoundInclusive();
  }

  public boolean hasUpperBound() {
    return flags.hasUpperBound();
  }

  @SuppressWarnings("unchecked")
  public T getUpperBound() {
    if (flags.hasLowerBound())
      return (T) values[1];
    return (T) values[0];
  }

  public void setUpperBound(T val) {
    values[1] = val;
    if (val == null) {
      flags.value |= Flags.RANGE_UB_NULL;
      if (!hasLowerBound())
        flags.value |= Flags.RANGE_EMPTY;
    }
  }

  public boolean isUpperBoundInfinity() {
    return flags.isUpperBoundInfinity();
  }

  public boolean isUpperBoundInclusive() {
    return flags.isUpperBoundInclusive();
  }

  @Override
  public String toString() {
    return "Range (" + flags.value + ") " + Arrays.toString(values);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((flags == null) ? 0 : flags.hashCode());
    result = prime * result + Arrays.hashCode(values);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Range<?> other = (Range<?>) obj;
    if (flags == null) {
      if (other.flags != null)
        return false;
    }
    else if (!flags.equals(other.flags))
      return false;
    if (!Arrays.equals(values, other.values))
      return false;
    return true;
  }

}
