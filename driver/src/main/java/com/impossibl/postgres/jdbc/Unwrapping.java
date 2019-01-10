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

import com.impossibl.postgres.utils.guava.ByteStreams;
import com.impossibl.postgres.utils.guava.CharStreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;


class Unwrapping {

  static Object unwrapObject(PGDirectConnection connection, Object x) throws SQLException {

    if (x instanceof Blob) {
      return unwrapBlob(connection, (Blob) x);
    }

    if (x instanceof Clob) {
      return unwrapClob(connection, (Clob) x);
    }

    if (x instanceof RowId) {
      return unwrapRowId((RowId) x);
    }

    if (x instanceof SQLXML) {
      return unwrapXML((SQLXML) x);
    }

    return x;
  }

  static PGSQLXML unwrapXML(SQLXML x) throws SQLException {

    if (x == null) {
      return null;
    }

    if (!(x instanceof PGSQLXML)) {
      throw new SQLException("SQLXML is from a different provider");
    }

    PGSQLXML xml = (PGSQLXML) x;

    if (xml.isNull()) {
      return null;
    }

    return xml;
  }

  static PGRowId unwrapRowId(RowId x) throws SQLException {

    if (x == null) {
      return null;
    }

    if (x instanceof PGRowId) {
      return (PGRowId) x;
    }

    throw new SQLException("RowId is from a different provider");
  }

  static PGBlob unwrapBlob(PGDirectConnection connection, Blob x) throws SQLException {

    if (x == null) {
      return null;
    }

    if (x instanceof PGBlob) {
      return (PGBlob) x;
    }

    InputStream in = x.getBinaryStream();
    if (in instanceof BlobInputStream) {
      return new PGBlob(connection, ((BlobInputStream) in).lo.oid);
    }

    PGBlob nx = (PGBlob) connection.createBlob();
    OutputStream out = nx.setBinaryStream(1);

    try {

      ByteStreams.copy(in, out);

      in.close();
      out.close();
    }
    catch (IOException e) {
      throw new SQLException(e);
    }

    return nx;
  }

  static PGClob unwrapClob(PGDirectConnection connection, Clob x) throws SQLException {

    if (x == null) {
      return null;
    }

    if (x instanceof PGClob)
      return (PGClob) x;

    Reader in = x.getCharacterStream();
    if (in instanceof ClobReader) {
      return new PGClob(connection, ((ClobReader) in).lo.oid);
    }

    PGClob nx = (PGClob) connection.createClob();
    Writer out = nx.setCharacterStream(1);

    try {

      CharStreams.copy(in, out);

      in.close();
      out.close();
    }
    catch (IOException e) {
      throw new SQLException(e);
    }

    return nx;
  }

}
