package com.alarm.eagle.cep.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Jackson deserializer for Flink {@link Time}.
 *
 * <p>JSON format: {@code {"unit":"MINUTES","size":15}} or {@code null}.
 *
 * <p>业务含义：规则 JSON 中窗口时长（{@code within} 的时间参数）的序列化器，
 * 把 {@code {unit, size}} 还原成 Flink {@link Time} 对象。
 */
public class TimeDeserializer extends JsonDeserializer<Time> {

    @Override
    public Time deserialize(JsonParser p, DeserializationContext ctx)
            throws IOException {
        if (p.currentToken() == JsonToken.VALUE_NULL) {
            return null;
        }

        String unit = null;
        long size = 0;

        while (p.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = p.currentName();
            p.nextToken();
            if ("unit".equals(fieldName)) {
                unit = p.getValueAsString();
            } else if ("size".equals(fieldName)) {
                size = p.getValueAsLong();
            }
        }

        if (unit == null) {
            return null;
        }
        return Time.of(size, TimeUnit.valueOf(unit));
    }
}
