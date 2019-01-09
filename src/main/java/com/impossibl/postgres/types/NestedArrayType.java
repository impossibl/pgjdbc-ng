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

import static com.impossibl.postgres.system.procs.NestedArrays.BINARY_DECODER;
import static com.impossibl.postgres.system.procs.NestedArrays.TEXT_DECODER;

/**
 * A synthetic type used for managing nested array decoding.
 *
 * @author kdubb
 *
 */
public class NestedArrayType extends ArrayType {

  private int[] dimensions;
  private FieldFormat elementFormat;

  public NestedArrayType(ArrayType arrayType, Type elementType, FieldFormat elementFormat, int[] dimensions) {
    super(
        arrayType.getId(), arrayType.getName(), arrayType.getNamespace(), arrayType.getLength(),
        arrayType.getAlignment(), arrayType.getCategory(), arrayType.getDelimeter(), arrayType.getArrayTypeId(),
        new BinaryCodec(BINARY_DECODER, null), new TextCodec(TEXT_DECODER, null),
        arrayType.getModifierParser(),
        elementFormat, elementFormat, elementType
    );
    this.dimensions = dimensions;
    this.elementFormat = elementFormat;
  }

  public int[] getDimensions() {
    return dimensions;
  }

  public FieldFormat getElementFormat() {
    return elementFormat;
  }

  @Override
  public String toString() {
    return super.toString() + "[]";
  }
}
