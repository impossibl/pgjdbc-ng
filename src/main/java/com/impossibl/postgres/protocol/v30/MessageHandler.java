package com.impossibl.postgres.protocol.v30;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;


public class MessageHandler extends SimpleChannelUpstreamHandler {

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		
		ResponseMessage msg = (ResponseMessage) e.getMessage();
		
		ProtocolImpl protocol = (ProtocolImpl) ctx.getChannel().getAttachment();
		protocol.dispatch(msg);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
		
		ProtocolImpl protocol = (ProtocolImpl) ctx.getChannel().getAttachment();
		if(protocol != null) {
			protocol.dispatchException(e.getCause());
		}
	}

}
