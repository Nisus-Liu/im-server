package com.xiaoleilu.loServer.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import lombok.extern.slf4j.Slf4j;

// 配置WebSocket协议处理器
@Slf4j
public class MqttWebSocketHandler extends SimpleChannelInboundHandler<BinaryWebSocketFrame> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, BinaryWebSocketFrame msg) {
        // // 转换WebSocket消息为MQTT协议格式
        // ByteBuf mqttBuffer = Unpooled.wrappedBuffer(msg.text().getBytes()); // 使用Netty的ByteBuf内存池减少GC
        // ctx.fireChannelRead(mqttBuffer);
        ByteBuf mqttBuffer = msg.content().retain(); // 直接传递二进制数据
        ctx.fireChannelRead(mqttBuffer);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Handler error:", cause);
        ctx.close();
    }

}
