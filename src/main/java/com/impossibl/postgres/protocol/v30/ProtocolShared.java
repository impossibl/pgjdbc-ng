package com.impossibl.postgres.protocol.v30;

import static java.lang.Runtime.getRuntime;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;



public class ProtocolShared {

	public class Ref {

		boolean released;

		public ProtocolShared get() {
			return ProtocolShared.this;
		}

		public void release() {
			if(!released) {
				released = true;
				ProtocolShared.this.release();
			}
		}

		@Override
		protected void finalize() {
			release();
		}

	}

	static ProtocolShared instance;

	public static synchronized Ref acquire() {
		if(instance == null) {
			instance = new ProtocolShared();
		}
		return instance.addReference();
	}

	private ClientBootstrap bootstrap;
	private int count = 0;

	public ClientBootstrap getBootstrap() {
		return bootstrap;
	}

	private synchronized Ref addReference() {
		if(count == 0) {
			init();
		}
		count++;
		return new Ref();
	}

	private synchronized void release() {
		if(count == 1) {
			shutdown();
			count = 0;
		}
		else {
			count--;
		}
	}

	private void init() {

		Executor bossExecutorService = Executors.newCachedThreadPool();
		Executor workerExecutorService = Executors.newCachedThreadPool();

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

}
