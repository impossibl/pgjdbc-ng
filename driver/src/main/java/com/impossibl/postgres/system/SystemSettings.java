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

import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.ssl.SSLMode;

import java.nio.charset.Charset;


@Setting.Factory
public class SystemSettings {

  @Setting.Group.Info(
      id = "system", desc = "System Settings", order = 3
  )
  public static final Setting.Group SYS = Setting.Group.declare();


  @Setting.Info(
      name = "database.url",
      group = "system",
      desc = "URL of database connection",
      alternateNames = {"url"}
  )
  public static final Setting<String> DATABASE_URL = Setting.declare();

  @Setting.Info(
      name = "database.name",
      group = "system",
      desc = "Name of database related to connection",
      def = "",
      alternateNames = {"database"}
  )
  public static final Setting<String> DATABASE_NAME = Setting.declare();

  @Setting.Info(
      name = "application.name",
      group = "system",
      desc = "Name of the client application",
      def = "Driver implementation name",
      defStatic = "com.impossibl.postgres.jdbc.PGDriver.NAME",
      alternateNames = {ParameterNames.APPLICATION_NAME}
  )
  public static final Setting<String> APPLICATION_NAME = Setting.declare();

  @Setting.Info(
      name = ParameterNames.USER,
      group = "system",
      desc =
          "Username for server authentication & authorization.\n\n" +
          "If no value is provided is defaults to the Java system property `user.name`.",
      def = "Current user via <code>user.name</code> system property",
      defDynamic = "System.getProperty(\"user.name\", \"\")"
  )
  public static final Setting<String> CREDENTIALS_USERNAME = Setting.declare();

  @Setting.Info(
      name = ParameterNames.PASSWORD,
      group = "system",
      desc = "Password for server authentication.",
      def = ""
  )
  public static final Setting<String> CREDENTIALS_PASSWORD = Setting.declare();

  @Setting.Info(
      name = "field.format",
      group = "system",
      desc = "Preferred format of result fields.",
      def = "binary"
  )
  public static final Setting<FieldFormat> FIELD_FORMAT_PREF = Setting.declare();

  @Setting.Info(
      name = "field.length.max",
      group = "system",
      min = 0,
      desc = "Default maximum allowed length of field."
  )
  public static final Setting<Integer> FIELD_LENGTH_MAX = Setting.declare();

  @Setting.Info(
      name = "param.format",
      group = "system",
      desc = "Preferred format of prepared statement parameters.",
      def = "binary"
  )
  public static final Setting<FieldFormat> PARAM_FORMAT_PREF = Setting.declare();

  @Setting.Info(
      name = "money.fractional-digits",
      group = "system",
      desc = "# of fractional digits for money fields.",
      def = "2",
      min = 0, max = 20,
      alternateNames = {"field.money.fractionalDigits"}
  )
  public static final Setting<Integer> MONEY_FRACTIONAL_DIGITS = Setting.declare();

  @Setting.Info(
      name = "ssl.mode",
      group = "system",
      desc = "SSL connection mode.",
      def = "disable",
      alternateNames = {"sslMode"}
  )
  public static final Setting<SSLMode> SSL_MODE = Setting.declare();

  @Setting.Info(
      name = "ssl.certificate.file",
      group = "system",
      desc = "SSL client certificate file name.",
      def = "postgresql.crt",
      alternateNames = {"sslCertificateFile"}
  )
  public static final Setting<String> SSL_CRT_FILE = Setting.declare();

  @Setting.Info(
      name = "ssl.ca.certificate.file",
      group = "system",
      desc = "SSL certificate authority file name.",
      def = "root.crt",
      alternateNames = {"sslRootCertificateFile"}
  )
  public static final Setting<String> SSL_CA_CRT_FILE = Setting.declare();

  @Setting.Info(
      desc = "SSL key file name.",
      def = "postgresql.pk8",
      name = "ssl.key.file",
      group = "system",
      alternateNames = {"sslKeyFile"}
  )
  public static final Setting<String> SSL_KEY_FILE = Setting.declare();

  @Setting.Info(
      desc = "SSL key file password.",
      name = "ssl.key.password",
      group = "system",
      alternateNames = {"sslPassword"}
  )
  public static final Setting<String> SSL_KEY_PASSWORD = Setting.declare();

  @Setting.Info(
      desc = "SSL key file password callback class name.",
      def = "com.impossibl.postgres.protocol.ssl.ConsolePasswordCallbackHandler",
      name = "ssl.key.password.callback",
      group = "system",
      alternateNames = {"sslPasswordCallback"}
  )
  public static final Setting<Class> SSL_KEY_PASSWORD_CALLBACK = Setting.declare();

  @Setting.Info(
      desc = "Factory for creating input streams for reading SSL files",
      def = "com.impossibl.postgres.protocol.ssl.SSLFileReaderFactory$Default",
      name = "ssl.file-reader-factory",
      group = "system",
      alternateNames = {"sslFileReaderFactory"}
  )
  public static final Setting<Class> SSL_FILE_READER_FACTORY = Setting.declare();

  @Setting.Info(
      desc =
          "Directory that SSL files are located in.\n\n" +
          "If the value begins with a path separator (e.g. `/`) it will be considered an absolute path. In all other \n" +
          "cases it is considered a value relative to the user's home directory.\nOn Windows `$APPDATA` is used as the \n" +
          "home directory, all others use the `user.home` system property.",
      def = ".postgresql",
      name = "ssl.home-dir",
      group = "system"
  )
  public static final Setting<String> SSL_HOME_DIR = Setting.declare();

  @Setting.Info(
      desc = "Enables or disables SQL trace output",
      def = "false",
      name = "sql.trace",
      group = "system"
  )
  public static final Setting<Boolean> SQL_TRACE = Setting.declare();

  @Setting.Info(
      desc =
          "File destination of SQL trace output.\n\n" +
          "NOTE: `sql.trace` must be `true` to generate trace output",
      name = "sql.trace.file",
      group = "system"
  )
  public static final Setting<String> SQL_TRACE_FILE = Setting.declare();




  @Setting.Group.Info(
      id = "protocol", desc = "Protocol Settings"
  )
  public static final Setting.Group PROTO = Setting.Group.declare();


  @Setting.Info(
      desc =
          "Version of server protocol to use.\n\n" +
          "Valid protocol versions:\n" +
          "<ul>\n" +
          "  <li><code>3.0</code></li>\n" +
          "</ul>\n",
      def = "3.0",
      name = "protocol.version",
      group = "protocol"
  )
  public static final Setting<Version> PROTOCOL_VERSION = Setting.declare();

  public enum ProtocolIOMode {

    @Setting.Description(
        "Attempts to use each other subsystem in order of <code>native</code>, <code>nio</code>, <code>oio</code>."
    )
    ANY,

    @Setting.Description(
        "Native subsystem using <b>kqueue</b> on macOS and <b>epoll</b> on Linux.\n" +
        "Native libraries must be provided, <a href=\"https://netty.io/wiki/native-transports.html\">see Netty's wiki</a>.\n"
    )
    NATIVE,

    @Setting.Description(
        "Java NIO subsystem."
    )
    NIO,

    @Setting.Description(
        "Java Blocking I/O subsystem."
    )
    OIO,
  }

  @Setting.Info(
      desc = "I/O subsystem selection mode.",
      def = "any",
      name = "protocol.io.mode",
      group = "protocol"
  )
  public static final Setting<ProtocolIOMode> PROTOCOL_IO_MODE = Setting.declare();

  @Setting.Info(
      desc = "Number of I/O threads in pool",
      def = "3",
      name = "protocol.io.threads",
      min = 1,
      group = "protocol"
  )
  public static final Setting<Integer> PROTOCOL_IO_THREADS = Setting.declare();


  @Setting.Info(
      desc = "Text encoding",
      def = "UTF-8",
      name = "protocol.encoding",
      group = "protocol",
      alternateNames = {"clientEncoding", ParameterNames.CLIENT_ENCODING}
  )
  public static final Setting<Charset> PROTOCOL_ENCODING = Setting.declare();

  @Setting.Info(
      desc = "Socket receive buffer size",
      name = "protocol.socket.recv-buffer.size",
      group = "protocol",
      min = 0,
      alternateNames = {"receiveBufferSize"}
  )
  public static final Setting<Integer> PROTOCOL_SOCKET_RECV_BUFFER_SIZE = Setting.declare();

  @Setting.Info(
      desc = "Socket send buffer size",
      name = "protocol.socket.send-buffer.size",
      group = "protocol",
      min = 0,
      alternateNames = {"sendBufferSize"}
  )
  public static final Setting<Integer> PROTOCOL_SOCKET_SEND_BUFFER_SIZE = Setting.declare();

  @Setting.Info(
      desc = "Enable or disable pooling of byte buffers",
      def = "true",
      name = "protocol.buffer.pooling",
      group = "protocol"
  )
  public static final Setting<Boolean> PROTOCOL_BUFFER_POOLING = Setting.declare();

  @Setting.Info(
      desc = "Maximum size message that can be received",
      def = "" + (20 * 1024 * 1024),
      name = "protocol.message.size.max",
      min = 0,
      group = "protocol"
  )
  public static final Setting<Integer> PROTOCOL_MESSAGE_SIZE_MAX = Setting.declare();

  @Setting.Info(
      desc = "Enable or disable message trace output",
      def = "false",
      name = "protocol.trace",
      group = "protocol"
  )
  public static final Setting<Boolean> PROTOCOL_TRACE = Setting.declare();

  @Setting.Info(
      desc =
          "File destination of message trace output\n\n" +
          "NOTE: `protocol.trace` must be `true` to generate trace output",
      name = "protocol.trace.file",
      group = "protocol"
  )
  public static final Setting<String> PROTOCOL_TRACE_FILE = Setting.declare();


  @Setting.Group.Info(
      id = "server", desc = "Server reported settings", global = false
  )
  public static final Setting.Group SERVER = Setting.Group.declare();

  @Setting.Info(
      desc = "Session username",
      name = ParameterNames.SESSION_AUTHORIZATION,
      group = "server"
  )
  public static final Setting<String> SESSION_USER = Setting.declare();

  @Setting.Info(
      desc = "Use SQL standard conforming strings",
      name = ParameterNames.STANDARD_CONFORMING_STRINGS,
      group = "server"
  )
  public static final Setting<Boolean> STANDARD_CONFORMING_STRINGS = Setting.declare();

  static {
    SystemSettingsInit.init();
  }

}
