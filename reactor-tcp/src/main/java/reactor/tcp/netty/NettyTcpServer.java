package reactor.tcp.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.fn.Consumer;
import reactor.fn.dispatch.Dispatcher;
import reactor.io.Buffer;
import reactor.support.NamedDaemonThreadFactory;
import reactor.tcp.AbstractTcpConnection;
import reactor.tcp.TcpConnection;
import reactor.tcp.TcpServer;
import reactor.tcp.encoding.Codec;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;

/**
 * @author Jon Brisbin
 */
public class NettyTcpServer<IN, OUT> extends TcpServer<IN, OUT> {

	private final ServerBootstrap bootstrap;
	private final Reactor         eventsReactor;
	private final int             memReclaimRatio;

	protected NettyTcpServer(Environment env,
													 Reactor reactor,
													 InetSocketAddress listenAddress,
													 int backlog,
													 int rcvbuf,
													 int sndbuf,
													 Codec<Buffer, IN, OUT> codec,
													 Collection<Consumer<TcpConnection<IN, OUT>>> connectionConsumers) {
		super(env, reactor, listenAddress, backlog, rcvbuf, sndbuf, codec, connectionConsumers);
		this.eventsReactor = reactor;

		memReclaimRatio = env.getProperty("reactor.tcp.memReclaimRatio", Integer.class, 12);
		int selectThreadCount = env.getProperty("reactor.tcp.selectThreadCount", Integer.class, Environment.PROCESSORS / 2);
		int ioThreadCount = env.getProperty("reactor.tcp.ioThreadCount", Integer.class, Environment.PROCESSORS);
		EventLoopGroup selectorGroup = new NioEventLoopGroup(selectThreadCount, new NamedDaemonThreadFactory("reactor-tcp-select"));
		EventLoopGroup ioGroup = new NioEventLoopGroup(ioThreadCount, new NamedDaemonThreadFactory("reactor-tcp-io"));

		this.bootstrap = new ServerBootstrap()
				.group(selectorGroup, ioGroup)
				.channel(NioServerSocketChannel.class)
				.option(ChannelOption.SO_BACKLOG, backlog)
				.option(ChannelOption.SO_RCVBUF, rcvbuf)
				.option(ChannelOption.SO_SNDBUF, sndbuf)
				.localAddress((null == listenAddress ? new InetSocketAddress(3000) : listenAddress))
				.handler(new LoggingHandler())
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(final SocketChannel ch) throws Exception {
						ch.pipeline().addLast(createChannelHandlers(ch));
						ch.closeFuture().addListener(new ChannelFutureListener() {
							@Override
							public void operationComplete(ChannelFuture future) throws Exception {
								close(ch);
							}
						});
					}

					@Override
					public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
						NettyTcpServer.this.notifyError(cause);
					}
				});
	}

	@Override
	public NettyTcpServer<IN, OUT> start(final Consumer<Void> started) {
		ChannelFuture bindFuture = bootstrap.bind();
		if (null != started) {
			bindFuture.addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					notifyStart(started);
				}
			});
		}

		return this;
	}

	@Override
	public TcpServer<IN, OUT> shutdown(Consumer<Void> stopped) {
		bootstrap.shutdown();
		notifyShutdown(stopped);
		return this;
	}

	@Override
	protected <C> NettyTcpConnection<IN, OUT> createConnection(C channel) {
		SocketChannel ch = (SocketChannel) channel;
		int backlog = env.getProperty("reactor.tcp.connectionReactorBacklog", Integer.class, 128);

		return new NettyTcpConnection<IN, OUT>(
				env,
				getCodec(),
				new NettyEventLoopDispatcher(ch.eventLoop(), backlog),
				eventsReactor,
				ch
		);
	}

	protected ChannelHandler[] createChannelHandlers(SocketChannel ch) {
		final NettyTcpConnection<IN, OUT> conn = (NettyTcpConnection<IN, OUT>) select(ch);

		ChannelHandler readHandler = new ChannelInboundHandlerAdapter() {
            @Override
            public void messageReceived(ChannelHandlerContext ctx, MessageList<Object> msgs) throws Exception {
                MessageList<ByteBuf> cast = msgs.cast();
                for (int i = 0; i < cast.size(); i++) {
                    ByteBuf data = cast.get(i);
                    Buffer b = new Buffer(data.nioBuffer());
                    conn.read(b);
                }
                msgs.releaseAllAndRecycle();
            }
        };
		return new ChannelHandler[]{readHandler};
	}

	protected class NettyTcpConnection<IN, OUT> extends AbstractTcpConnection<IN, OUT> {
		private final SocketChannel     channel;
		private final InetSocketAddress remoteAddress;

		public NettyTcpConnection(Environment env,
															Codec<Buffer, IN, OUT> codec,
															Dispatcher ioDispatcher,
															Reactor eventsReactor,
															SocketChannel channel) {
			super(env, codec, ioDispatcher, eventsReactor);
			this.channel = channel;
			this.remoteAddress = channel.remoteAddress();
		}

		@Override
		public boolean consumable() {
			return !channel.isInputShutdown();
		}

		@Override
		public boolean writable() {
			return !channel.isOutputShutdown();
		}

		@Override
		public InetSocketAddress remoteAddress() {
			return remoteAddress;
		}

		@Override
		protected void write(Buffer data, final Consumer<Boolean> onComplete) {
			write(data.asBytes(), onComplete);
		}

		@Override
		protected void write(Object data, final Consumer<Boolean> onComplete) {
			ChannelFuture writeFuture = channel.write(data);
			if (null != onComplete) {
				writeFuture.addListener(new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture future) throws Exception {
						onComplete.accept(future.isSuccess());
					}
				});
			}
		}
	}

	private static class LoggingHandler extends ChannelOutboundHandlerAdapter {
		private final Logger LOG = LoggerFactory.getLogger(NettyTcpServer.class);

		@Override
		public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise future) throws Exception {
			if (LOG.isInfoEnabled()) {
				LOG.info("BIND {}", localAddress);
			}
			super.bind(ctx, localAddress, future);
		}

		@Override
		public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise future) throws Exception {
			if (LOG.isDebugEnabled()) {
				LOG.debug("CONNECT {}", remoteAddress);
			}
			super.connect(ctx, remoteAddress, localAddress, future);
		}
	}

}
