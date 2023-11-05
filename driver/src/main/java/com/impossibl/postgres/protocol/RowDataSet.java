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
package com.impossibl.postgres.protocol;

import java.util.ArrayList;
import java.util.List;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;

public class RowDataSet extends AbstractReferenceCounted {

  private List<RowData> rows;

  public RowDataSet() {
    this(0);
  }

  public RowDataSet(int capacity) {
    this.rows = new ArrayList<>(capacity);
  }

  public boolean isEmpty() {
    return rows.isEmpty();
  }

  public int size() {
    return rows.size();
  }

  public RowData borrow(int index) {
    return rows.get(index);
  }

  public void add(RowData row) {
    rows.add(row);
  }

  public RowData copy(int index) {
    return ReferenceCountUtil.retain(borrow(index));
  }

  public RowData take(int index) {
    return rows.remove(index);
  }

  public void remove(int index) {
    ReferenceCountUtil.release(rows.remove(index));
  }

  public List<RowData> borrowAll() {
    return rows;
  }

  @Override
  protected void deallocate() {
    rows.forEach(ReferenceCountUtil::release);
  }

  @Override
  public ReferenceCounted touch(Object hint) {
    rows.forEach(rowData -> ReferenceCountUtil.touch(rowData, hint));
    return this;
  }

  @Override
  public String toString() {
    if (rows.size() == 0) {
      return "Empty";
    }
    return rows.size() + " Rows";
  }

}
