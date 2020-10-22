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
package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.jdbc.PGDirectConnection;
import com.impossibl.postgres.jdbc.PGStatement;
import com.impossibl.postgres.protocol.RequestExecutorHandlers;
import com.impossibl.postgres.protocol.ResultBatch;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;

import io.netty.buffer.ByteBuf;

public class RefCursors extends SimpleProcProvider {

  static final BinDecoder BINARY_DECODER = new BinDecoder();
  static final TxtDecoder TEXT_DECODER = new TxtDecoder();

  public RefCursors() {
    super(null, TEXT_DECODER, null, BINARY_DECODER, "refcursor");
  }

  private static Object convertOutput(Context context, ResultSet decoded, Class<?> targetClass, Object targetContext) {

    if (targetClass == ResultSet.class) {
      return decoded;
    }

    return null;
  }

  private static ResultSet resultSet(PGDirectConnection connection, String portalName) throws IOException {
    try {

      RequestExecutorHandlers.QueryResult result = new RequestExecutorHandlers.QueryResult(true);

      connection.getRequestExecutor().query(portalName, 0, result);

      result.await(180, SECONDS);

      ResultBatch batch = result.getBatch();

      PGStatement statement = connection.createStatement();
      statement.closeOnCompletion();

      return statement.createResultSet(batch.getFields(), batch.takeRows(), true, connection.getTypeMap());

    }
    catch (SQLException e) {
      throw new IOException("Failed to load refcursor", e);
    }
  }

  public static class BinDecoder extends AutoConvertingBinaryDecoder<ResultSet> {

    public BinDecoder() {
      super(null, RefCursors::convertOutput);
    }

    @Override
    public Class<ResultSet> getDefaultClass() {
      return ResultSet.class;
    }

    @Override
    protected ResultSet decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, ByteBuf buffer, Class<?> targetClass, Object targetContext) throws IOException {

      int length = buffer.readableBytes();
      byte[] bytes = new byte[length];

      buffer.readBytes(bytes);

      String portalName = new String(bytes, UTF_8);

      PGDirectConnection connection = (PGDirectConnection) context.unwrap();

      return resultSet(connection, portalName);
    }

  }

  public static class TxtDecoder extends AutoConvertingTextDecoder<ResultSet> {

    TxtDecoder() {
      super(RefCursors::convertOutput);
      enableRespectMaxLength();
    }

    @Override
    public Class<ResultSet> getDefaultClass() {
      return ResultSet.class;
    }

    @Override
    protected ResultSet decodeNativeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException, ParseException {

      String portalName = buffer.toString();

      PGDirectConnection connection = (PGDirectConnection) context.unwrap();

      return resultSet(connection, portalName);
    }

  }

}
