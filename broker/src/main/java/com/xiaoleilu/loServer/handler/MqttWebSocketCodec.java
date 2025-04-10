package com.xiaoleilu.loServer.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
public class MqttWebSocketCodec extends MessageToMessageDecoder<WebSocketFrame> {
    private final MqttDecoder mqttDecoder = new MqttDecoder();

    @Override
    protected void decode(ChannelHandlerContext ctx, WebSocketFrame frame, List<Object> out) {
        log.info("WebSocket frame received: {}", frame.getClass());
        try {
            if (frame instanceof BinaryWebSocketFrame) {
                ByteBuf buf = frame.content();
                out.add(buf.retain()); // 将WebSocket二进制帧转换为MQTT能处理的ByteBuf
            } else if (frame instanceof TextWebSocketFrame) {
                // 兼容文本帧（有些MQTT over WS实现会发文本帧）
                ByteBuf buf = Unpooled.copiedBuffer(((TextWebSocketFrame)frame).text(), StandardCharsets.UTF_8);
                out.add(buf);
            }
        } finally {
            frame.release();
        }
        /*if (frame instanceof BinaryWebSocketFrame) {
            ByteBuf buf = frame.content();
            // 确保缓冲区可读
            if (buf.readableBytes() < 2) {
                log.warn("Invalid MQTT packet length");
                return;
            }

            // 检查保留位（第一个字节的低3位）
            byte firstByte = buf.getByte(buf.readerIndex());
            if ((firstByte & 0x0F) != 0) { // 检查保留位是否为0
                log.warn("MQTT reserved bits not zero: {}", String.format("%02X", firstByte));
                // 强制修正保留位
                buf.setByte(buf.readerIndex(), firstByte & 0xF0); // 清除低4位中的保留位
            }
            out.add(buf.retain());
        }*/
    }
}
