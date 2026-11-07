package com.alarm.eagle.cep;

import com.alarm.eagle.cep.operator.DynamicCepOperatorFactory;
import com.alarm.eagle.cep.processor.PatternProcessorDiscovererFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * Dynamic CEP API entry point (counterpart to the official {@code CEP.java}).
 *
 * <p>在实时流中的定位：对外唯一入口，把"事件流 + 规则发现器"组装成一个
 * 带热更新能力的 CEP 算子（{@code DynamicCepOperator}）。
 *
 * <p>设计目的：对齐官方 {@code CEP.pattern()} 的调用习惯，让使用方无需感知
 * JM 协调器（{@code DynamicCepCoordinator}）与 TM 算子之间的 OperatorEvent 通信细节。
 *
 * <p>Usage:
 * <pre>{@code
 * SingleOutputStreamOperator<String> output = DynamicCEP.dynamicPatterns(
 *     keyedStream,
 *     new JDBCPeriodicPatternProcessorDiscovererFactory<>(
 *         jdbcUrl, jdbcDriver, tableName, null, intervalMs),
 *     false,  // isProcessingTime
 *     TypeInformation.of(new TypeHint<String>() {}));
 * }</pre>
 */
public class DynamicCEP {

    /**
     * ★ Create a dynamic CEP stream.
     *
     * @param input             输入事件流（交易事件）；若已 keyBy 则直接复用，
     *                          否则自动 {@code keyBy(NullByteKeySelector)} 并强制单并行度
     * @param discovererFactory 规则发现器工厂，负责在 JM 侧轮询规则并触发热更新
     * @param isProcessingTime  {@code true}=处理时间语义；{@code false}=事件时间语义
     * @param outTypeInfo       输出类型信息（告警结果流的类型）
     * @param <T>               事件类型（本模块为 {@code Transaction}）
     * @param <R>               输出类型（本模块为 {@code String} 告警文本）
     * @return CEP 匹配结果流（下游为告警输出/sink）
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T, R> SingleOutputStreamOperator<R> dynamicPatterns(
            DataStream<T> input,
            PatternProcessorDiscovererFactory<T> discovererFactory,
            boolean isProcessingTime,
            TypeInformation<R> outTypeInfo) {

        DynamicCepOperatorFactory<T, R> factory =
                new DynamicCepOperatorFactory<>(
                        discovererFactory,
                        input.getType().createSerializer(
                                input.getExecutionConfig()),
                        isProcessingTime);

        // Generic type inference may fail across interface hierarchies; use raw type to bypass
        DataStream rawStream = input;
        if (rawStream instanceof KeyedStream) {
            return ((KeyedStream) rawStream)
                    .transform("DynamicCepOperator", outTypeInfo,
                            (org.apache.flink.streaming.api.operators
                                    .OneInputStreamOperatorFactory) factory);
        }
        return rawStream.keyBy(new NullByteKeySelector())
                .transform("DynamicCepOperator", outTypeInfo,
                        (org.apache.flink.streaming.api.operators
                                .OneInputStreamOperatorFactory) factory)
                .forceNonParallel();
    }
}
