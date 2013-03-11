package com.impossibl.postgres.protocol.v30;

import java.io.IOException;
import java.net.SocketAddress;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import com.impossibl.postgres.protocol.Protocol;
import com.impossibl.postgres.protocol.ProtocolFactory;
import com.impossibl.postgres.system.BasicContext;



public class ProtocolFactoryImpl implements ProtocolFactory {

	@Override
	public Protocol connect(SocketAddress address, BasicContext context) throws IOException {

		ProtocolShared.Ref sharedRef = ProtocolShared.acquire();

		ChannelFuture channelFuture = sharedRef.get().getBootstrap().connect(address).awaitUninterruptibly();
		if(!channelFuture.isSuccess()) {
			throw new IOException(channelFuture.getCause());
		}

		Channel channel = channelFuture.getChannel();
		Protocol protocol = new ProtocolImpl(sharedRef, channel, context);

		channel.setAttachment(protocol);

		return protocol;
	}

}
