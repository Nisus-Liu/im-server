/*
 * Copyright (c) 2012-2017 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.moquette.spi.impl;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import java.util.Map;

/**
 * Utility static methods, like Map get with default value, or elvis operator.
 */
public final class Utils {

    public static <T, K> T defaultGet(Map<K, T> map, K key, T defaultValue) {
        T value = map.get(key);
        if (value != null) {
            return value;
        }
        return defaultValue;
    }

    public static int messageId(MqttMessage msg) {
        return ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
    }

    public static byte[] readBytesAndRewind(ByteBuf payload) { // rewind：重播
        byte[] payloadContent = new byte[payload.readableBytes()];
        int mark = payload.readerIndex(); // 备份当前读取位置
        payload.readBytes(payloadContent);
        payload.readerIndex(mark); // 本次读取会移动 readerIndex 为了下游还能重复读，将 payload 的读取索引重置为 mark，以便后续可以重新读取这些字节。
        return payloadContent;
    }

    private Utils() {
    }
}
