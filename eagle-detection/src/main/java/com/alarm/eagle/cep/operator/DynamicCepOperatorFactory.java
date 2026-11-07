package com.alarm.eagle.cep.operator;

import com.alarm.eagle.cep.processor.PatternProcessorDiscovererFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.util.Preconditions;

/**
 * Factory class for DynamicCepOperator.
 *
 * <p>Implements {@link CoordinatedOperatorFactory}, supporting JM-side Coordinator and
 * TM-side Operator OperatorEvent communication.
 *
 * <p>业务含义：既负责在 TM 侧创建 {@link DynamicCepOperator} 并注册事件通道，
 * 又负责在 JM 侧提供协调器，是打通"规则下发"这条双向链路的关键工厂。
 */
public class DynamicCepOperatorFactory<IN, OUT>
        extends AbstractStreamOperatorFactory<OUT>
        implements OneInputStreamOperatorFactory<IN, OUT>,
                   CoordinatedOperatorFactory<OUT> {

    private static final long serialVersionUID = 1L;

    private final PatternProcessorDiscovererFactory<IN> discovererFactory;
    private final TypeSerializer<IN> inputSerializer;
    private final boolean isProcessingTime;

    public DynamicCepOperatorFactory(
            PatternProcessorDiscovererFactory<IN> discovererFactory,
            TypeSerializer<IN> inputSerializer,
            boolean isProcessingTime) {
        this.discovererFactory = Preconditions.checkNotNull(discovererFactory);
        this.inputSerializer = Preconditions.checkNotNull(inputSerializer);
        this.isProcessingTime = isProcessingTime;
    }

    /**
     * 在 TM 侧创建 {@link DynamicCepOperator} 实例，并完成三件初始化：
     * 手动注入 ProcessingTimeService（AbstractStreamOperator 不自动注入）、
     * 注册为 OperatorEventHandler（接收 JM 规则事件）。
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters) {
        OperatorID operatorId =
                parameters.getStreamConfig().getOperatorID();

        DynamicCepOperator<IN, ?, OUT> operator =
                new DynamicCepOperator<>(inputSerializer, isProcessingTime);

        operator.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());

        // ★ Explicitly set ProcessingTimeService (AbstractStreamOperator V1 does not auto-inject)
        operator.setProcessingTimeService(
                parameters.getProcessingTimeService());

        // ★ Register as OperatorEventHandler to receive Coordinator events
        parameters.getOperatorEventDispatcher()
                .registerEventHandler(operatorId, operator);

        return (T) operator;
    }

    /**
     * 提供 JM 侧协调器的 Provider，使 Flink 能为该算子实例化 {@link DynamicCepCoordinator}。
     */
    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        return new DynamicCepCoordinatorProvider<>(
                operatorName, operatorID, discovererFactory);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(
            ClassLoader classLoader) {
        return DynamicCepOperator.class;
    }
}
