package reactor.tcp.syslog;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.string.StringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import reactor.core.Environment;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.Buffer;
import reactor.tcp.TcpConnection;
import reactor.tcp.TcpServer;
import reactor.tcp.encoding.syslog.SyslogCodec;
import reactor.tcp.encoding.syslog.SyslogMessage;
import reactor.tcp.netty.NettyTcpServer;
import reactor.tcp.syslog.hdfs.HdfsConsumer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * @author Jon Brisbin
 */
public class SyslogTcpServerTests {

	static final byte[] SYSLOG_MESSAGE_DATA = "<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8\n".getBytes();

	final int msgs    = 2000000;
	final int threads = 4;

	Environment    env;
	CountDownLatch latch;
	AtomicLong count = new AtomicLong();
	AtomicLong start = new AtomicLong();
	AtomicLong end   = new AtomicLong();

	@Before
	public void loadEnv() {
		env = new Environment();
		latch = new CountDownLatch(msgs * threads);
	}

	@Test
	public void testSyslogServer() throws InterruptedException, IOException {
		EventLoopGroup bossGroup = new NioEventLoopGroup(Environment.PROCESSORS / 2);
		EventLoopGroup workerGroup = new NioEventLoopGroup( Environment.PROCESSORS);

		Configuration conf = new Configuration();
		conf.addResource("/usr/local/Cellar/hadoop/1.1.2/libexec/conf/core-site.xml");
		final HdfsConsumer hdfs = new HdfsConsumer(conf, "loadtests", "syslog");

		ServerBootstrap b = new ServerBootstrap();
		b.group(bossGroup, workerGroup)
         .option(ChannelOption.SO_BACKLOG, 512)
         .option(ChannelOption.SO_RCVBUF, 8 * 1024)
         .option(ChannelOption.SO_SNDBUF, 8 * 1024)
		 .localAddress(3000)
		 .channel(NioServerSocketChannel.class)
		 .childHandler(new ChannelInitializer<SocketChannel>() {
			 @Override
			 public void initChannel(SocketChannel ch) throws Exception {
				 ChannelPipeline pipeline = ch.pipeline();
				 pipeline.addLast("framer", new LineBasedFrameDecoder(8192, false, false));
				 pipeline.addLast("syslogDecoder", new MessageToMessageDecoder<ByteBuf>() {
					 Function<Buffer, SyslogMessage> decoder = new SyslogCodec().decoder(null, null);

					 @Override
					 public void decode(ChannelHandlerContext ctx, ByteBuf msg, MessageList<Object> out) throws Exception {
                         ByteBuffer buffer = msg.nioBuffer();
                         int position = buffer.position();
                         SyslogMessage syslogMessage = decoder.apply(new Buffer(buffer));
                         int read = buffer.position() - position;
                         msg.readerIndex(msg.readerIndex() + read);
				         out.add(syslogMessage);
					 }
				 });
				 pipeline.addLast("handler", new ChannelInboundHandlerAdapter() {

                     @Override
                     public void messageReceived(ChannelHandlerContext ctx, MessageList<Object> msgs) throws Exception {
                         MessageList<SyslogMessage> cast = msgs.cast();
                         for (int i = 0; i < cast.size(); i++) {
                             latch.countDown();
                             hdfs.accept(cast.get(i));
                         }
                         msgs.releaseAllAndRecycle();
                     }

                     @Override
                     public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                         cause.printStackTrace();
                         ctx.close();
                     }
                 });
			 }
		 });

		// Bind and start to accept incoming connections.
		ChannelFuture channelFuture = b.bind().awaitUninterruptibly();

		for (int i = 0; i < threads; i++) {
			new SyslogMessageWriter(3000).start();
		}

		latch.await(60, TimeUnit.SECONDS);
		end.set(System.currentTimeMillis());

		assertThat("latch was counted down", latch.getCount(), is(0L));

		double elapsed = (end.get() - start.get()) * 1.0;
		System.out.println("elapsed: " + (int) elapsed + "ms");
		System.out.println("throughput: " + (int) ((msgs * threads) / (elapsed / 1000)) + "/sec");

		channelFuture.channel().close().awaitUninterruptibly();
	}

	@Test
	public void testTcpSyslogServer() throws InterruptedException, IOException {
		//final FileChannelConsumer<SyslogMessage> fcc = new FileChannelConsumer<SyslogMessage>(".", "syslog", -1, -1);
		Configuration conf = new Configuration();
		conf.addResource("/usr/local/Cellar/hadoop/1.1.2/libexec/conf/core-site.xml");
		final HdfsConsumer hdfs = new HdfsConsumer(conf, "loadtests", "syslog");


		TcpServer<SyslogMessage, Void> server = new TcpServer.Spec<SyslogMessage, Void>(NettyTcpServer.class)
				.using(env)
						//.using(SynchronousDispatcher.INSTANCE)
						//.dispatcher(Environment.EVENT_LOOP)
				.dispatcher(Environment.RING_BUFFER)
				.codec(new SyslogCodec())
				.consume(new Consumer<TcpConnection<SyslogMessage, Void>>() {
					@Override
					public void accept(TcpConnection<SyslogMessage, Void> conn) {
						conn
								.consume(new Consumer<SyslogMessage>() {
									@Override
									public void accept(SyslogMessage msg) {
										count.incrementAndGet();
									}
								})
								.consume(hdfs);
					}
				})
				.get()
				.start(
						new Consumer<Void>() {
							@Override
							public void accept(Void v) {
								for (int i = 0; i < threads; i++) {
									new SyslogMessageWriter(3000).start();
								}
							}
						}
				);

		while (count.get() < (msgs * threads)) {
			end.set(System.currentTimeMillis());
			Thread.sleep(100);
		}

		double elapsed = (end.get() - start.get()) * 1.0;
		System.out.println("elapsed: " + (int) elapsed + "ms");
		System.out.println("throughput: " + (int) ((msgs * threads) / (elapsed / 1000)) + "/sec");

		server.shutdown();
	}

	@Test
	public void testExternalServer() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);

		long start = System.currentTimeMillis();
		SyslogMessageWriter[] writers = new SyslogMessageWriter[threads];
		for (int i = 0; i < threads; i++) {
			writers[i] = new SyslogMessageWriter(5140);
			writers[i].start();
		}

		latch.await(10, TimeUnit.SECONDS);
		// Calculate exact time, which will be slightly over timeout
		long end = System.currentTimeMillis();
		double elapsed = (end - start) * 1.0;

		int totalMsgs = 0;
		for (int i = 0; i < threads; i++) {
			totalMsgs += writers[i].count.intValue();
		}

		System.out.println("throughput: " + (int) ((totalMsgs) / (elapsed / 1000)) + "/sec");
	}

	private class SyslogMessageWriter extends Thread {
		AtomicLong count = new AtomicLong();
		private final int port;

		private SyslogMessageWriter(int port) {
			this.port = port;
		}

		@Override
		public void run() {
			try {
				java.nio.channels.SocketChannel ch = java.nio.channels.SocketChannel.open(new InetSocketAddress(port));

				start.set(System.currentTimeMillis());
				for (int i = 0; i < msgs; i++) {
					ch.write(ByteBuffer.wrap(SYSLOG_MESSAGE_DATA));
					count.incrementAndGet();
				}
			} catch (IOException e) {
			}
		}
	}

}
