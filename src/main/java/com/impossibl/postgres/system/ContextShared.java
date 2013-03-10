package com.impossibl.postgres.system;

import static java.lang.Runtime.getRuntime;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.impossibl.postgres.protocol.Protocol;
import com.impossibl.postgres.protocol.v30.MessageDecoder;
import com.impossibl.postgres.protocol.v30.MessageHandler;

public class ContextShared {

	private ClientBootstrap bootstrap;
	private int count = 0;

	private void init() {
		
		Executor bossExecutorService = Executors.newCachedThreadPool(new ContextThreadFactory("PostgresSQL JDBC - Boss"));
		Executor workerExecutorService = Executors.newCachedThreadPool(new ContextThreadFactory("PostgresSQL JDBC - Worker"));
		
		int workerCount = getRuntime().availableProcessors();

		ChannelFactory channelFactory = new NioClientSocketChannelFactory(bossExecutorService, workerExecutorService, workerCount);

		bootstrap = new ClientBootstrap(channelFactory);
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {

			@Override
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(new MessageDecoder(), new MessageHandler());
			}
			
		});
		
	}
	
	private void shutdown() {
		
		bootstrap.shutdown();
		
		bootstrap.releaseExternalResources();
	}
	
	public synchronized void increment() {
		if(count == 0) {
			init();
		}
		count++;
	}
	
	public synchronized void decrement() {
		if(count == 1) {
			shutdown();
			count = 0;
		}
		else {
			count--;
		}
	}
	
	public Channel connect(SocketAddress address, Protocol protocol) throws IOException {
		
		increment();
		
		ChannelFuture channelFuture = bootstrap.connect(address).awaitUninterruptibly();
		if(!channelFuture.isSuccess()) {
			throw new IOException(channelFuture.getCause());
		}
		
		Channel channel = channelFuture.getChannel();
		channel.setAttachment(protocol);
		return channel;
	}
	
	public void disconnect(Channel channel) {
		
		channel.disconnect().awaitUninterruptibly();
		
		decrement();
	}
	
}

class ContextThreadFactory implements ThreadFactory {

	String name;
	AtomicInteger threadIdx = new AtomicInteger(0);
	
	public ContextThreadFactory(String name) {
		super();
		this.name = name;
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread t = new Thread(r, name + " (" + (threadIdx.incrementAndGet()) + ")");
		t.setDaemon(true);
		return t;
	}
	
}
