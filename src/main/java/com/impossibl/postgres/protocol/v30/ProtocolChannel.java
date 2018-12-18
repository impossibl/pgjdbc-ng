package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.ServerObjectType;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.protocol.FieldFormat.Text;
import static com.impossibl.postgres.utils.ByteBufs.lengthEncode;
import static com.impossibl.postgres.utils.ByteBufs.writeCString;
import static com.impossibl.postgres.utils.guava.Strings.nullToEmpty;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.logging.Logger;

import static java.util.logging.Level.FINEST;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;

public class ProtocolChannel {

  private static final Logger LOGGER = Logger.getLogger(ProtocolChannel.class.getName());

  // Frontend messages
  private static final byte PASSWORD_MSG_ID = 'p';
  private static final byte FLUSH_MSG_ID = 'H';
  private static final byte TERMINATE_MSG_ID = 'X';
  private static final byte SYNC_MSG_ID = 'S';
  private static final byte QUERY_MSG_ID = 'Q';
  private static final byte PARSE_MSG_ID = 'P';
  private static final byte BIND_MSG_ID = 'B';
  private static final byte DESCRIBE_MSG_ID = 'D';
  private static final byte EXECUTE_MSG_ID = 'E';
  private static final byte CLOSE_MSG_ID = 'C';
  private static final byte FUNCTION_CALL_MSG_ID = 'F';

  private Channel channel;
  private Charset charset;

  public ProtocolChannel(Channel channel, Charset charset) {
    this.channel = channel;
    this.charset = charset;
  }

  ChannelPipeline pipeline() {
    return channel.pipeline();
  }

  ProtocolChannel flush() {
    channel.flush();
    return this;
  }

  ProtocolChannel writeSSLRequest() {

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("SSL");

    ByteBuf msg = channel.alloc().buffer();

    msg.writeInt(8);
    msg.writeInt(80877103);

    channel.write(msg);

    return this;
  }

  ProtocolChannel writeStartup(Map<String, Object> params) {

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("STARTUP: " + params);

    ByteBuf msg = channel.alloc().buffer();

    beginMessage(msg, (byte) 0);

    // Version
    msg.writeShort(3);
    msg.writeShort(0);

    // Name=Value pairs
    for (Map.Entry<String, Object> paramEntry : params.entrySet()) {
      writeCString(msg, paramEntry.getKey(), charset);
      writeCString(msg, paramEntry.getValue().toString(), charset);
    }

    msg.writeByte(0);

    endMessage(msg);

    channel.write(msg);

    return this;
  }

  ProtocolChannel writePassword(String password) {

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("PASSWORD: " + password);

    ByteBuf msg = channel.alloc().buffer();

    beginMessage(msg, PASSWORD_MSG_ID);

    writeCString(msg, password, charset);

    endMessage(msg);

    channel.write(msg);

    return this;
  }

  ProtocolChannel writeQuery(String query) {

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("QUERY: " + query);

    ByteBuf msg = channel.alloc().buffer();

    beginMessage(msg, QUERY_MSG_ID);

    writeCString(msg, query, charset);

    endMessage(msg);

    channel.write(msg);

    return this;
  }

  ProtocolChannel writeParse(String stmtName, String query, Type[] paramTypes) {

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("PARSE (" + stmtName + "): " + query);

    ByteBuf msg = channel.alloc().buffer();

    beginMessage(msg, PARSE_MSG_ID);

    writeCString(msg, stmtName != null ? stmtName : "", charset);
    writeCString(msg, query, charset);

    msg.writeShort(paramTypes.length);
    for (Type paramType : paramTypes) {
      int paramTypeOid = paramType != null ? paramType.getId() : 0;
      msg.writeInt(paramTypeOid);
    }

    endMessage(msg);

    channel.write(msg);

    return this;
  }

  private boolean isAllText(FieldFormatRef[] fieldFormats) {
    return fieldFormats.length == 1 && fieldFormats[0].getFormat() == Text;
  }

  ProtocolChannel writeBind(String portalName, String stmtName, FieldFormatRef[] parameterFormats, ByteBuf[] parameterBuffers, FieldFormatRef[] resultFieldFormatRefs) throws IOException {

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("BIND (" + portalName + "): " + parameterBuffers.length);

    byte[] portalNameBytes = nullToEmpty(portalName).getBytes(charset);
    byte[] stmtNameBytes = nullToEmpty(stmtName).getBytes(charset);

    ByteBuf msg = channel.alloc().buffer();

    beginMessage(msg, BIND_MSG_ID);

    writeCString(msg, portalNameBytes);
    writeCString(msg, stmtNameBytes);

    loadParams(msg, parameterFormats, parameterBuffers);

    //Set format for results fields
    if (resultFieldFormatRefs == null || resultFieldFormatRefs.length == 0) {
      //Request all binary
      msg.writeShort(1);
      msg.writeShort(1);
    }
    else if (isAllText(resultFieldFormatRefs)) {
      //Shortcut to all text
      msg.writeShort(0);
    }
    else if (!isAllText(resultFieldFormatRefs)) {
      //Select result format for each
      msg.writeShort(resultFieldFormatRefs.length);
      for (FieldFormatRef formatRef : resultFieldFormatRefs) {
        msg.writeShort(formatRef.getFormat().ordinal());
      }
    }

    endMessage(msg);

    channel.write(msg);

    return this;
  }

  ProtocolChannel writeDescribe(ServerObjectType target, String targetName) {

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("DESCRIBE " + target + " (" + targetName + ")");

    ByteBuf msg = channel.alloc().buffer();

    beginMessage(msg, DESCRIBE_MSG_ID);

    msg.writeByte(target.getId());
    writeCString(msg, targetName != null ? targetName : "", charset);

    endMessage(msg);

    channel.write(msg);

    return this;
  }

  ProtocolChannel writeExecute(String portalName, int maxRows) {

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("EXECUTE (" + portalName + "): " + maxRows);

    ByteBuf msg = channel.alloc().buffer();

    beginMessage(msg, EXECUTE_MSG_ID);

    writeCString(msg, portalName != null ? portalName : "", charset);
    msg.writeInt(maxRows);

    endMessage(msg);

    channel.write(msg);

    return this;
  }

  ProtocolChannel writeFunctionCall(int functionId, FieldFormatRef[] parameterFormatRefs, ByteBuf[] parameterBuffers) throws IOException {

    ByteBuf msg = channel.alloc().buffer();

    beginMessage(msg, FUNCTION_CALL_MSG_ID);

    msg.writeInt(functionId);

    loadParams(msg, parameterFormatRefs, parameterBuffers);

    msg.writeShort(1);

    endMessage(msg);

    channel.write(msg);

    return this;
  }

  ProtocolChannel writeClose(ServerObjectType target, String targetName) {

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("CLOSE " + target + ": " + targetName);

    ByteBuf msg = channel.alloc().buffer();

    beginMessage(msg, CLOSE_MSG_ID);

    msg.writeByte(target.getId());
    writeCString(msg, targetName != null ? targetName : "", charset);

    endMessage(msg);

    channel.write(msg);

    return this;
  }

  ProtocolChannel writeFlush() {

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("FLUSH");

    ByteBuf msg = channel.alloc().buffer();

    writeMessage(msg, FLUSH_MSG_ID);

    channel.write(msg);

    return this;
  }

  ProtocolChannel writeSync() {

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("SYNC");

    ByteBuf msg = channel.alloc().buffer();

    writeMessage(msg, SYNC_MSG_ID);

    channel.write(msg);

    return this;
  }

  ChannelFuture writeTerminate() {

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("TERM");

    ByteBuf msg = channel.alloc().buffer();

    writeMessage(msg, TERMINATE_MSG_ID);

    return channel.writeAndFlush(msg);
  }

  private void writeMessage(ByteBuf msg, byte msgId) {

    msg.writeByte(msgId);
    msg.writeInt(4);
  }

  private void beginMessage(ByteBuf msg, byte msgId) {

    if (msgId != 0)
      msg.writeByte(msgId);

    msg.markWriterIndex();

    msg.writeInt(-1);
  }

  private void endMessage(ByteBuf msg) {

    int endPos = msg.writerIndex();

    msg.resetWriterIndex();

    int begPos = msg.writerIndex();

    msg.setInt(begPos, endPos - begPos);

    msg.writerIndex(endPos);
  }

  private void loadParams(ByteBuf msg, FieldFormatRef[] fieldFormatRefs, ByteBuf[] paramBuffers) throws IOException {

    // Select format for parameters
    if (fieldFormatRefs == null) {
      msg.writeShort(1);
      msg.writeShort(1);
    }
    else {
      msg.writeShort(fieldFormatRefs.length);
      for (FieldFormatRef paramFormatRef : fieldFormatRefs) {
        paramFormatRef = paramFormatRef != null ? paramFormatRef : FieldFormat.Text;
        msg.writeShort(paramFormatRef.getFormat().ordinal());
      }
    }

    // Values for each parameter
    if (paramBuffers == null) {
      msg.writeShort(0);
    }
    else {
      msg.writeShort(paramBuffers.length);
      for (ByteBuf paramBuffer : paramBuffers) {
        lengthEncode(msg, paramBuffer, () -> {
          msg.writeBytes(paramBuffer);
          paramBuffer.resetReaderIndex();
        });
      }
    }
  }

}
