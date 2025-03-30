package com.xiaoleilu.loServer.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.MqttMessage;
import lombok.extern.slf4j.Slf4j;

/**
 * @author nisus
 * @since 2025/3/30 19:27
 */
@Slf4j
public class MqttServerHandler extends SimpleChannelInboundHandler<MqttMessage> {

    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) {
        log.info("MqttServerHandler {}", msg);
        switch (msg.fixedHeader().messageType()) {
            // case CONNECT:
            //     handleConnect(ctx, (MqttConnectMessage) msg);
            //     break;
            // case SUBSCRIBE:
            //     handleSubscribe(ctx, (MqttSubscribeMessage) msg);
            //     break;
            // case PUBLISH:
            //     handlePublish(ctx, (MqttPublishMessage) msg);
            //     break;
            // 其他协议处理...
        }
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
    // }


}
