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

import com.impossibl.postgres.jdbc.PGDriver;
import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.ssl.ConsolePasswordCallbackHandler;
import com.impossibl.postgres.protocol.ssl.SSLMode;
import com.impossibl.postgres.utils.StringTransforms;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static java.util.function.Function.identity;


public class SystemSettings implements Setting.Provider {

  public static final Setting.Group SYS = new Setting.Group("system");

  public static final Setting<String> DATABASE_URL = SYS.add(
      "Database URL",
      (String) null,
      "databaseUrl", "url"
  );

  public static final Setting<String> DATABASE_NAME = SYS.add(
      "Name of database related to connection",
      (String) null,
      "databaseName", "database"
  );

  public static final Setting<String> APPLICATION_NAME = SYS.add(
      "Name of the client application",
      PGDriver.NAME,
      "application.name", "applicationName", ParameterNames.APPLICATION_NAME
  );

  public static final Setting<String> CREDENTIALS_USERNAME = SYS.<String>add(
      "Username",
      String.class, () -> System.getProperty("user.name", ""), String::toString, identity(),
      ParameterNames.USER
  );

  public static final Setting<String> CREDENTIALS_PASSWORD = SYS.add(
      "Password",
      "",
      ParameterNames.PASSWORD
  );

  public static final Setting<String> SESSION_USER = SYS.add(
      "Session username (as reported by server)",
      (String) null,
      ParameterNames.SESSION_USER
  );

  public static final Setting<Boolean> STANDARD_CONFORMING_STRINGS = SYS.add(
      "Use SQL standard conforming strings (as reported by server)",
      (Boolean) null,
      ParameterNames.STANDARD_CONFORMING_STRINGS
  );

  public static final Setting<FieldFormat> FIELD_FORMAT_PREF = SYS.add(
      "Preferred format (text or binary) of result fields",
      FieldFormat.Binary, StringTransforms::capitalizeOption,
      "field.format.preference"
  );

  public static final Setting<Integer> FIELD_MONEY_FRACTIONAL_DIGITS = SYS.add(
      "Fractional digits for money fields",
      2,
      "field.money.fractionalDigits"
  );

  public static final Setting<Integer> FIELD_VARYING_LENGTH_MAX = SYS.add(
      "Fractional digits for money fields",
      (Integer) null,
      "field.varying.length.max"
  );

  public static final Setting<FieldFormat> PARAM_FORMAT_PREF = SYS.add(
      "Preferred format (text or binary) of prepared statement parameters",
      FieldFormat.Binary, StringTransforms::capitalizeOption,
      "param.format.preference"
  );

  public static final Setting<SSLMode> SSL_MODE = SYS.add(
      "SSL connection mode",
      SSLMode.Disable, StringTransforms::capitalizeOption,
      "ssl.mode", "sslMode"
  );

  public static final Setting<String> SSL_CRT_FILE = SYS.add(
      "SSL certificate file name",
      "postgresql.crt",
      "ssl.certificate", "sslCertificateFile"
  );

  public static final Setting<String> SSL_KEY_FILE = SYS.add(
      "SSL key file name",
      "postgresql.pk8",
      "ssl.key", "sslKeyFile"
  );

  public static final Setting<String> SSL_KEY_FILE_PASSWORD = SYS.add(
      "SSL key file password",
      (String) null,
      "ssl.password", "sslPassword"
  );

  public static final Setting<Class> SSL_KEY_FILE_PASSWORD_CALLBACK = SYS.add(
      "SSL key file password callback class",
      ConsolePasswordCallbackHandler.class,
      "ssl.password.callback.class", "sslPasswordCallback"
  );

  public static final Setting<String> SSL_ROOT_CRT_FILE = SYS.add(
      "SSL root certificate file name",
      "root.crt",
      "ssl.certificate.ca", "sslRootCertificateFile"
  );

  public static final Setting<String> SSL_HOME_DIR = SYS.add(
      "SSL user home directory",
      "postgresql",
      "ssl.dir"
  );

  public enum IOMode {
    ANY,
    NIO,
    NATIVE,
    OIO,
  }

  public static final Setting<IOMode> PROTOCOL_IO = SYS.add(
      "I/O subsystem selection mode (any, native, nio, oio)",
      IOMode.ANY, String::toUpperCase,
      "protocol.io"
  );

  public static final Setting<Integer> PROTOCOL_IO_THREADS = SYS.add(
      "Number of I/O threads in pool",
      3,
      "protocol.io.threads"
  );

  public static final Setting<Version> PROTOCOL_VERSION = SYS.add(
      "Version",
      Version.class, Version.parse("3.0"), Version::parse, Version::toString,
      "protocol.version"
  );

  public static final Setting<Charset> PROTOCOL_ENCODING = SYS.add(
      "Text encoding",
      Charset.class, StandardCharsets.UTF_8, Charset::forName, Charset::name,
      "protocol.encoding", "clientEncoding", ParameterNames.CLIENT_ENCODING
  );

  public static final Setting<Integer> PROTOCOL_SOCKET_RECV_BUFFER_SIZE = SYS.add(
      "Socket receive buffer size",
      (Integer) null,
      "protocol.socket.receiveBuffer", "receiveBufferSize"
  );

  public static final Setting<Integer> PROTOCOL_SOCKET_SEND_BUFFER_SIZE = SYS.add(
      "Socket send buffer size",
      (Integer) null,
      "protocol.socket.sendBuffer", "sendBufferSize"
  );

  public static final Setting<Boolean> PROTOCOL_ALLOCATOR_POOLED = SYS.add(
      "Enable or disable use of pooled byte buffer allocator",
      true,
      "protocol.allocator.pooled"
  );

  public static final Setting<Integer> PROTOCOL_MAX_MESSAGE_SIZE = SYS.add(
      "Maximum size message that can be received",
      20 * 1024 * 1024,
      "protocol.message.max"
  );

  public static final Setting<Boolean> PROTOCOL_TRACE = SYS.add(
      "Enable or disable message trace output",
      false,
      "protocol.trace"
  );

  public static final Setting<Boolean> SQL_TRACE = SYS.add(
      "Enables or disables SQL trace output",
      false,
      "sql.trace"
  );

}
