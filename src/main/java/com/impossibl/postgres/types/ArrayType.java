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
import com.impossibl.postgres.system.procs.Procs;
import com.impossibl.postgres.system.tables.PGTypeTable;

import java.io.IOException;

/**
 * A database array type.
 *
 * @author kdubb
 *
 */
public class ArrayType extends Type {

  private Type elementType;

  public ArrayType() {
  }

  public ArrayType(int id, String name, Short length, Byte alignment, Category category, char delimeter, int arrayTypeId, String procName, Procs procs, FieldFormat preferredParameterFormat, FieldFormat preferredResultFormat, Type elementType) {
    super(id, name, CATALOG_NAMESPACE, length, alignment, category, delimeter, arrayTypeId, procs.loadNamedBinaryCodec(procName), procs.loadNamedTextCodec(procName), procs.loadModifierParserProc(procName), preferredParameterFormat, preferredResultFormat);
    this.elementType = elementType;
  }

  public ArrayType(int id, String name, Short length, Byte alignment, Category category, char delimeter, int arrayTypeId, Procs procs, FieldFormat preferredParameterFormat, FieldFormat preferredResultFormat, Type elementType) {
    this(id, name, length, alignment, category, delimeter, arrayTypeId, "array_", procs, preferredParameterFormat, preferredResultFormat, elementType);
  }

  public ArrayType(int id, String name, String namespace, Short length, Byte alignment, Category category, char delimeter, int arrayTypeId, BinaryCodec binaryCodec, TextCodec textCodec, Modifiers.Parser modifierParser, FieldFormat preferredParameterFormat, FieldFormat preferredResultFormat, Type elementType) {
    super(id, name, namespace, length, alignment, category, delimeter, arrayTypeId, binaryCodec, textCodec, modifierParser, preferredParameterFormat, preferredResultFormat);
    this.elementType = elementType;
  }

  public Type getElementType() {
    return elementType;
  }

  @Override
  public PrimitiveType getPrimitiveType() {
    return PrimitiveType.Array;
  }

  @Override
  public boolean isParameterFormatSupported(FieldFormat format) {
    return elementType.isParameterFormatSupported(format);
  }

  @Override
  public boolean isResultFormatSupported(FieldFormat format) {
    return elementType.isResultFormatSupported(format);
  }

  @Override
  public Type unwrap() {
    return elementType;
  }

  @Override
  public void load(PGTypeTable.Row source, Registry registry) throws IOException {
    super.load(source, registry);
    this.elementType = registry.loadType(source.getElementTypeId());
  }

}
