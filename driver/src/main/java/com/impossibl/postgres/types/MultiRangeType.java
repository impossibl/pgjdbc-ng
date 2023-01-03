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
package com.impossibl.postgres.types;

import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.system.tables.PGTypeTable;

import java.io.IOException;

/**
 * A database multi-range type.
 *
 * @author Dave Cramer
 *
 */
public class MultiRangeType extends Type {

  Type base;
  public Type getBase() {
    return base;
  }

  public void setBase(Type base) {
    this.base = base;
  }

  @Override
  public boolean isParameterFormatSupported(FieldFormat format) {
    return base.isParameterFormatSupported(format);
  }

  @Override
  public boolean isResultFormatSupported(FieldFormat format) {
    return base.isResultFormatSupported(format);
  }

  @Override
  public Type unwrap() {
    return this;
  }

  @Override
  public void load(PGTypeTable.Row source, Registry registry) throws IOException {
    super.load(source, registry);
    base = registry.loadType(source.getMultiRangeBaseTypeId());

  }
}
