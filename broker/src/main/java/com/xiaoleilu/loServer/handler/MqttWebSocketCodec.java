package com.xiaoleilu.loServer.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class MqttWebSocketCodec extends MessageToMessageDecoder<WebSocketFrame> {
    @Override
    protected void decode(ChannelHandlerContext ctx, WebSocketFrame frame, List<Object> out) {
        log.info("WebSocket frame received: {}", frame.getClass());
        if (frame instanceof BinaryWebSocketFrame) {
            ByteBuf buf = frame.content();
            out.add(buf.retain()); // 传递原始 MQTT 数据
        }
    }
}
