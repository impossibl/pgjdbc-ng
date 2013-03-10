package com.impossibl.postgres.protocol.v30;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;
import org.jboss.netty.handler.codec.replay.VoidEnum;

import com.impossibl.postgres.protocol.ResponseMessage;

public class MessageDecoder extends ReplayingDecoder<VoidEnum> {

	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer, VoidEnum state) throws Exception {
		
		byte id = buffer.readByte();
		int length = buffer.readInt() - 4;
		
		ChannelBuffer dataBuffer = buffer.readBytes(length);
		
		return new ResponseMessage(id, dataBuffer);
	}

}
