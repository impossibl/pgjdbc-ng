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
package com.impossibl.postgres.system;

import com.impossibl.postgres.api.data.ACLItem;
import com.impossibl.postgres.api.data.CidrAddr;
import com.impossibl.postgres.api.data.InetAddr;
import com.impossibl.postgres.api.data.Interval;
import com.impossibl.postgres.api.data.Range;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.RowId;
import java.sql.SQLData;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.BitSet;
import java.util.Map;
import java.util.UUID;

/**
 * Provides mapping from Java classes to the best matching
 * PostgreSQL type
 */
public class JavaTypeMapping {

  public static Type getType(Class<?> cls, Registry reg) {
    if (cls == Boolean.class) {
      return reg.loadBaseType("bool");
    }
    if (cls == Byte.class) {
      return reg.loadBaseType("int2");
    }
    if (cls == Short.class) {
      return reg.loadBaseType("int2");
    }
    if (cls == Integer.class) {
      return reg.loadBaseType("int4");
    }
    if (cls == Long.class) {
      return reg.loadBaseType("int8");
    }
    if (cls == BigInteger.class) {
      return reg.loadBaseType("numeric");
    }
    if (cls == Float.class) {
      return reg.loadBaseType("float4");
    }
    if (cls == Double.class) {
      return reg.loadBaseType("float8");
    }
    if (cls == BigDecimal.class) {
      return reg.loadBaseType("numeric");
    }
    if (Number.class.isAssignableFrom(cls)) {
      return reg.loadBaseType("numeric");
    }
    if (cls == Character.class) {
      return reg.loadBaseType("char");
    }
    if (cls == String.class) {
      return reg.loadBaseType("text");
    }
    if (cls == Date.class) {
      return reg.loadBaseType("date");
    }
    if (cls == Time.class) {
      return reg.loadBaseType("time");
    }
    if (cls == Timestamp.class) {
      return reg.loadBaseType("timestamp");
    }
    if (cls == byte[].class) {
      return reg.loadBaseType("bytea");
    }
    if (InputStream.class.isAssignableFrom(cls)) {
      return reg.loadBaseType("bytea");
    }
    if (Reader.class.isAssignableFrom(cls)) {
      return reg.loadBaseType("text");
    }
    if (Blob.class.isAssignableFrom(cls)) {
      return reg.loadBaseType("loid");
    }
    if (Clob.class.isAssignableFrom(cls)) {
      return reg.loadBaseType("loid");
    }
    if (Array.class.isAssignableFrom(cls)) {
      return reg.loadBaseType("anyarray");
    }
    if (Struct.class.isAssignableFrom(cls)) {
      return reg.loadBaseType("record");
    }
    if (SQLData.class.isAssignableFrom(cls)) {
      return reg.loadBaseType("record");
    }
    if (SQLXML.class.isAssignableFrom(cls)) {
      return reg.loadBaseType("xml");
    }
    if (RowId.class.isAssignableFrom(cls)) {
      return reg.loadBaseType("tid");
    }
    return getExtendedType(cls, reg);
  }

  public static Type getExtendedType(Class<?> cls, Registry reg) {
    if (cls == Interval.class) {
      return reg.loadBaseType("interval");
    }
    else if (cls == UUID.class) {
      return reg.loadBaseType("uuid");
    }
    else if (cls == Map.class) {
      return reg.loadStableType("hstore");
    }
    else if (cls == BitSet.class) {
      return reg.loadBaseType("bits");
    }
    else if (cls == Range.class) {
      return reg.loadBaseType("range");
    }
    else if (cls == ACLItem.class) {
      return reg.loadBaseType("aclitem");
    }
    else if (cls == CidrAddr.class) {
      return reg.loadBaseType("cidr");
    }
    else if (cls == InetAddr.class) {
      return reg.loadBaseType("inet");
    }
    return null;
  }

}
