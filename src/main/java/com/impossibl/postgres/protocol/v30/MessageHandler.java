package com.impossibl.postgres.protocol.v30;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;


public class MessageHandler extends SimpleChannelUpstreamHandler {

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		
		ResponseMessage msg = (ResponseMessage) e.getMessage();
		
		ProtocolImpl protocol = (ProtocolImpl) ctx.getChannel().getAttachment();
		protocol.dispatch(msg);
	}

}
