package com.xiaoleilu.loServer;

import cn.hutool.core.date.DateUtil;
import com.xiaoleilu.loServer.action.Action;
import com.xiaoleilu.loServer.annotation.Route;
import com.xiaoleilu.loServer.handler.AdminActionHandler;
import com.xiaoleilu.loServer.handler.MqttServerHandler1;
import com.xiaoleilu.loServer.handler.MqttWebSocketCodec;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.ISessionsStore;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import com.xiaoleilu.loServer.action.ClassUtil;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.LoggerFactory;
import win.liyufan.im.Utility;

import java.io.IOException;
import java.util.List;

/**
 * LoServer starter<br>
 * 用于启动服务器的主对象<br>
 * 使用LoServer.start()启动服务器<br>
 * 服务的Action类和端口等设置在ServerSetting中设置
 * @author Looly
 *
 */
public class LoServer {
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(LoServer.class);
	private int port;
    private int adminPort;
    private IMessagesStore messagesStore;
    private ISessionsStore sessionsStore;
    private Channel channel;
    private Channel adminChannel;

    public LoServer(int port, int adminPort, IMessagesStore messagesStore, ISessionsStore sessionsStore) {
        this.port = port;
        this.adminPort = adminPort;
        this.messagesStore = messagesStore;
        this.sessionsStore = sessionsStore;
    }

    /**
	 * 启动服务
	 * @throws InterruptedException
	 */
	public void start() throws InterruptedException {
		long start = System.currentTimeMillis();

		// Configure the server.
		final EventLoopGroup bossGroup = new NioEventLoopGroup(2);
		final EventLoopGroup workerGroup = new NioEventLoopGroup();

        registerAllAction(); // 注册所有Action，扫描Action包下Action类，解析注解，放进Map

        int bindingPort = port;
		try {
			final ServerBootstrap b = new ServerBootstrap(); // IM服务 Netty
            final ServerBootstrap adminB = new ServerBootstrap(); // 管理服务 Netty
			b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 10240) // 服务端可连接队列大小
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF, 1024*64)
                .childOption(ChannelOption.SO_RCVBUF, 1024*64)
                .childHandler(new ChannelInitializer<SocketChannel>() { //: 添加自定义 Handler
                    /*@Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        socketChannel.pipeline().addLast(new HttpRequestDecoder());
                        socketChannel.pipeline().addLast(new HttpResponseEncoder());
                        socketChannel.pipeline().addLast(new ChunkedWriteHandler());
                        socketChannel.pipeline().addLast(new HttpObjectAggregator(100 * 1024 * 1024));
                        socketChannel.pipeline().addLast(new IMActionHandler(messagesStore, sessionsStore));
					}*/
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new LoggingHandler(LogLevel.TRACE)); // 打印协议日志
                        // HTTP协议支持
                        pipeline.addLast(new HttpServerCodec());
                        pipeline.addLast(new HttpObjectAggregator(65536));
                        pipeline.addLast(new ChunkedWriteHandler());

                        // WebSocket协议处理
                        // pipeline.addLast(new WebSocketServerProtocolHandler("/", "mqtt", true));
                        // pipeline.addLast(new WebSocketServerProtocolHandler(
                        //         "/",                // websocketPath
                        //         "mqtt",             // subprotocols
                        //         true,               // allowExtensions
                        //         65536,             // maxFrameSize
                        //         true,              // allowMaskMismatch
                        //         false               // performMasking
                        // ));
                        // 自定义WebSocket协议处理器（跳过保留位检查）
                        /*pipeline.addLast(new WebSocketServerProtocolHandler(
                                "/",
                                "mqtt",
                                true*//*,
                                false*//*
                        ) {
                            @Override
                            protected void decode(ChannelHandlerContext ctx, WebSocketFrame frame, List<Object> out) throws Exception {
                                // 强制清除所有保留位
                                if (frame instanceof BinaryWebSocketFrame) {
                                    BinaryWebSocketFrame newFrame = new BinaryWebSocketFrame(true, 0, frame.content());
                                    super.decode(ctx, newFrame, out);
                                } else {
                                    ctx.close();
                                }
                            }
                        });*/
                        pipeline.addLast(new WebSocketServerProtocolHandler(
                                "/",  // websocket path
                                null, // 不检查子协议
                                true  // 允许扩展
                        ) {
                            @Override
                            protected void decode(ChannelHandlerContext ctx, WebSocketFrame frame, List<Object> out) {
                                // 强制接受所有WebSocket帧
                                if (frame instanceof BinaryWebSocketFrame || frame instanceof TextWebSocketFrame) {
                                    out.add(frame.retain());
                                }
                            }
                        });
                        pipeline.addLast(new MqttWebSocketCodec()); // 需要实现这个类

                        // MQTT协议处理
                        pipeline.addLast(new MqttDecoder(256 * 1024));
                        pipeline.addLast(MqttEncoder.INSTANCE);
                        pipeline.addLast(new MqttServerHandler1());

                        // 空闲检测
                        pipeline.addLast(new IdleStateHandler(60, 0, 0));
                        // WebSocket帧 → BinaryWebSocketFrame → ByteBuf → MqttDecoder → MqttMessage
                    }
                    /*protected void initChannel(SocketChannel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();

                        // HTTP协议解码器
                        pipeline.addLast(new HttpServerCodec());
                        pipeline.addLast(new HttpObjectAggregator(65536));

                        // WebSocket 握手 + CORS
                        pipeline.addLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                if (msg instanceof FullHttpRequest) {
                                    HttpHeaders headers = ((FullHttpRequest) msg).headers();
                                    headers.set("Access-Control-Allow-Origin", "*");
                                    headers.set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
                                }
                                ctx.fireChannelRead(msg);
                            }
                        });

                        // WebSocket协议处理器
                        // pipeline.addLast(new WebSocketServerProtocolHandler("/mqtt"));
                        pipeline.addLast(new WebSocketServerProtocolHandler("/",  "mqtt", true)); // 修改为显式指定子协议（如 "mqtt"）
                        pipeline.addLast(new MqttWebSocketCodec()); // 转换 WebSocket 帧为 MQTT 协议
                        // 自定义协议转换
                        pipeline.addLast(new MqttWebSocketHandler());
                        // MQTT协议编解码
                        pipeline.addLast(MqttEncoder.INSTANCE);
                        pipeline.addLast(new MqttDecoder());
                        // 业务逻辑处理器
                        pipeline.addLast(new MqttServerHandler());
                        // pipeline.addLast(new IMActionHandler(messagesStore, sessionsStore));
                        pipeline.addLast(new IdleStateHandler(60, 0, 0)); // 60秒读空闲检测
                        pipeline.addLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                                if (evt instanceof IdleStateEvent) {
                                    ctx.writeAndFlush(new PingWebSocketFrame()); // 发送PING帧
                                }
                            }
                        });
                        pipeline.addLast(new LoggingHandler(LogLevel.TRACE)); // 打印协议日志
                    }*/
				});

			channel = b.bind(port).sync().channel();


            bindingPort = adminPort;
            adminB.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 10240) // 服务端可连接队列大小
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF, 1024*64)
                .childOption(ChannelOption.SO_RCVBUF, 1024*64)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        socketChannel.pipeline().addLast(new HttpRequestDecoder());
                        socketChannel.pipeline().addLast(new HttpResponseEncoder());
                        socketChannel.pipeline().addLast(new ChunkedWriteHandler());
                        socketChannel.pipeline().addLast(new HttpObjectAggregator(100 * 1024 * 1024));
                        socketChannel.pipeline().addLast(new AdminActionHandler(messagesStore, sessionsStore));
                    }
                });

            adminChannel = adminB.bind(adminPort).sync().channel(); //: 管理端口18080就会走到AdminActionHandler
			Logger.info("***** Welcome To LoServer on port [{},{}], starting spend {}ms *****", port, adminPort, DateUtil.spendMs(start));
        } catch (Exception e) {
            e.printStackTrace();
            Logger.error("端口 {} 已经被占用。请检查该端口被那个程序占用，找到程序停掉。\n查找端口被那个程序占用的命令是: netstat -tunlp | grep {}", bindingPort, bindingPort);
            System.out.println("端口 " + bindingPort + " 已经被占用。请检查该端口被那个程序占用，找到程序停掉。\n查找端口被那个程序占用的命令是: netstat -tunlp | grep " + bindingPort);
            System.exit(-1);
        }
	}

    public void shutdown() {
        if (this.channel!= null) {
            this.channel.close();
            try {
                this.channel.closeFuture().sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (this.adminChannel != null) {
            this.adminChannel.close();
            try {
                this.adminChannel.closeFuture().sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void registerAllAction() {
        try {
            for (Class cls:ClassUtil.getAllAssignedClass(Action.class) // 扫描所有 Action 类所在包（嵌套）下Action的子类
                 ) {
                if(cls.getAnnotation(Route.class) != null) {
                    ServerSetting.setAction((Class<? extends Action>)cls);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            Utility.printExecption(Logger, e);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
