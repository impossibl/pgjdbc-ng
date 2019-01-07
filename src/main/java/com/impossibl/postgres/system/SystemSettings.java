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

  public static final Setting.Group SYS = new Setting.Group(
      "system", "General system & connection settings"
  );

  public static final Setting<String> DATABASE_URL = SYS.add(
      "Database URL",
      (String) null,
      "database.url", "databaseUrl", "url"
  );

  public static final Setting<String> DATABASE_NAME = SYS.add(
      "Name of database related to connection",
      (String) null,
      "database.name", "databaseName", "database"
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

  public static final Setting<FieldFormat> FIELD_FORMAT_PREF = SYS.add(
      "Preferred format (text or binary) of result fields",
      FieldFormat.Binary, StringTransforms::toUpperCamelCase, String::toLowerCase,
      "field.format"
  );

  public static final Setting<Integer> FIELD_LENGTH_MAX = SYS.add(
      "Maximum allowed length of a result field",
      (Integer) null,
      "field.length.max"
  );

  public static final Setting<FieldFormat> PARAM_FORMAT_PREF = SYS.add(
      "Preferred format (text or binary) of prepared statement parameters",
      FieldFormat.Binary, StringTransforms::toUpperCamelCase, String::toLowerCase,
      "param.format"
  );

  public static final Setting<Integer> MONEY_FRACTIONAL_DIGITS = SYS.add(
      "Fractional digits for money fields",
      2,
      "money.fractional-digits", "field.money.fractionalDigits"
  );

  public static final Setting<SSLMode> SSL_MODE = SYS.add(
      "SSL connection mode",
      SSLMode.Disable, StringTransforms::toUpperCamelCase, StringTransforms::dashedFromCamelCase,
      "ssl-mode", "sslMode"
  );

  public static final Setting<String> SSL_CRT_FILE = SYS.add(
      "SSL certificate file name",
      "postgresql.crt",
      "ssl.certificate.file", "sslCertificateFile"
  );

  public static final Setting<String> SSL_CA_CRT_FILE = SYS.add(
      "SSL certificate authority file name",
      "root.crt",
      "ssl.ca.certificate.file", "sslRootCertificateFile"
  );

  public static final Setting<String> SSL_KEY_FILE = SYS.add(
      "SSL key file name",
      "postgresql.pk8",
      "ssl.key.file", "sslKeyFile"
  );

  public static final Setting<String> SSL_KEY_PASSWORD = SYS.add(
      "SSL key file password",
      (String) null,
      "ssl.key.password", "sslPassword"
  );

  public static final Setting<Class> SSL_KEY_PASSWORD_CALLBACK = SYS.add(
      "SSL key file password callback class",
      ConsolePasswordCallbackHandler.class,
      "ssl.key.password.callback", "sslPasswordCallback"
  );

  public static final Setting<String> SSL_HOME_DIR = SYS.add(
      "SSL user home directory",
      "postgresql",
      "ssl.home-dir"
  );

  public static final Setting<Boolean> SQL_TRACE = SYS.add(
      "Enables or disables SQL trace output",
      false,
      "sql.trace"
  );




  public static final Setting.Group PROTO = new Setting.Group(
      "protocol", "Server protocol settings"
  );

  public static final Setting<Version> PROTOCOL_VERSION = PROTO.add(
      "Version",
      Version.class, Version.parse("3.0"), Version::parse, Version::toString,
      "protocol.version"
  );

  public enum ProtocolIOMode {
    ANY,
    NIO,
    NATIVE,
    OIO,
  }

  public static final Setting<ProtocolIOMode> PROTOCOL_IO_MODE = PROTO.add(
      "I/O subsystem selection mode (any, native, nio, oio)",
      ProtocolIOMode.ANY, String::toUpperCase, String::toLowerCase,
      "protocol.io.mode"
  );

  public static final Setting<Integer> PROTOCOL_IO_THREADS = PROTO.add(
      "Number of I/O threads in pool",
      3,
      "protocol.io.threads"
  );


  public static final Setting<Charset> PROTOCOL_ENCODING = PROTO.add(
      "Text encoding",
      Charset.class, StandardCharsets.UTF_8, Charset::forName, Charset::name,
      "protocol.encoding", "clientEncoding", ParameterNames.CLIENT_ENCODING
  );

  public static final Setting<Integer> PROTOCOL_SOCKET_RECV_BUFFER_SIZE = PROTO.add(
      "Socket receive buffer size",
      (Integer) null,
      "protocol.socket.recv-buffer.size", "receiveBufferSize"
  );

  public static final Setting<Integer> PROTOCOL_SOCKET_SEND_BUFFER_SIZE = PROTO.add(
      "Socket send buffer size",
      (Integer) null,
      "protocol.socket.send-buffer.size", "sendBufferSize"
  );

  public static final Setting<Boolean> PROTOCOL_BUFFER_POOLING = PROTO.add(
      "Enable or disable pooling of byte buffers",
      true,
      "protocol.buffer.pooling"
  );

  public static final Setting<Integer> PROTOCOL_MESSAGE_SIZE_MAX = PROTO.add(
      "Maximum size message that can be received",
      20 * 1024 * 1024,
      "protocol.message.size.max"
  );

  public static final Setting<Boolean> PROTOCOL_TRACE = PROTO.add(
      "Enable or disable message trace output",
      false,
      "protocol.trace"
  );




  public static final Setting.Group SERVER = new Setting.Group(
      "server", "Server reported settings"
  );

  public static final Setting<String> SESSION_USER = SERVER.add(
      "Session username",
      (String) null,
      ParameterNames.SESSION_AUTHORIZATION
  );

  public static final Setting<Boolean> STANDARD_CONFORMING_STRINGS = SERVER.add(
      "Use SQL standard conforming strings",
      (Boolean) null,
      ParameterNames.STANDARD_CONFORMING_STRINGS
  );

}
