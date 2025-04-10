package com.xiaoleilu.loServer.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.extern.slf4j.Slf4j;

/**
 * @author nisus
 * @since 2025/3/30 19:27
 */
@Slf4j
public class MqttServerHandler1 extends SimpleChannelInboundHandler<MqttMessage> {

    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) {
        log.info("MqttServerHandler {}", msg.fixedHeader());
        // First check for null fixed header
        if (msg.fixedHeader() == null) {
            log.error("Received MQTT message with null fixed header");
            // ctx.close(); // Close the connection as this is protocol violation
            sendError(ctx, "Invalid MQTT message");
            return;
        }
        // ctx.channel().writeAndFlush("hello".getBytes());
        // MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        // MqttConnAckVariableHeader variableHeader = new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, true);
        // ctx.writeAndFlush(new MqttConnAckMessage(fixedHeader, variableHeader)); // 标准CONNACK响应
        switch (msg.fixedHeader().messageType()) {
            case CONNECT:
                handleConnect(ctx, (MqttConnectMessage) msg);
                break;
            case PUBLISH:
                handlePublish(ctx, (MqttPublishMessage) msg);
                break;
            case PINGREQ:
                // 返回PINGRESP
                ctx.writeAndFlush(new MqttMessage(new MqttFixedHeader(
                        MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0)));
                break;
            default:
                log.warn("Unhandled message type: {}", msg.fixedHeader().messageType());
            // 其他消息类型...
        }

        // switch (msg.fixedHeader().messageType()) {
        //     // case CONNECT:
        //     //     handleConnect(ctx, (MqttConnectMessage) msg);
        //     //     break;
        //     // case SUBSCRIBE:
        //     //     handleSubscribe(ctx, (MqttSubscribeMessage) msg);
        //     //     break;
        //     // case PUBLISH:
        //     //     handlePublish(ctx, (MqttPublishMessage) msg);
        //     //     break;
        //     // 其他协议处理...
        // }
    }

    private void handleConnect(ChannelHandlerContext ctx, MqttConnectMessage msg) {
        // 连接认证逻辑
        MqttConnAckMessage ack = new MqttConnAckMessage(
                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, true));
        ctx.writeAndFlush(ack);
        log.info("Client connected: {}", msg.payload().clientIdentifier());
    }

    private void handlePublish(ChannelHandlerContext ctx, MqttPublishMessage msg) {
        // 消息处理逻辑
        String topic = msg.variableHeader().topicName();
        ByteBuf payload = msg.payload();
        // 存储或转发消息...
    }

    // private void handlePublish(ChannelHandlerContext ctx, MqttPublishMessage msg) {
    //     String topic = msg.variableHeader().topicName();
    //     ByteBuf payload = msg.payload();
    //
    //     // 存储消息到Redis缓存
    //     // redisTemplate.opsForValue().set(topic, payload.toString(StandardCharsets.UTF_8));
    //
    //     // 转发给订阅客户端
    //     subscribers.get(topic).forEach(sub -> {
    //         sub.writeAndFlush(new TextWebSocketFrame(payload.toString(UTF_8)));
    //     });


    private void sendError(ChannelHandlerContext ctx, String reason) {
        log.error(reason);
        ctx.close();
    }

}
