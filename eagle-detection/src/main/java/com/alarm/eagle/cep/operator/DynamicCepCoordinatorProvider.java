package com.alarm.eagle.cep.operator;

import com.alarm.eagle.cep.processor.PatternProcessorDiscovererFactory;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

/**
 * Coordinator Provider — creates a {@link DynamicCepCoordinator} instance on the JM side.
 *
 * <p>业务含义：该 Provider 随 JobGraph 序列化到 JobManager，
 * 由 Flink 在需要时调用 {@link #create} 实例化协调器，是"TM 算子工厂"与"JM 协调器"之间的桥接。
 */
public class DynamicCepCoordinatorProvider<IN>
        implements OperatorCoordinator.Provider {

    private static final long serialVersionUID = 1L;

    private final String operatorName;
    private final OperatorID operatorID;
    private final PatternProcessorDiscovererFactory<IN> discovererFactory;

    public DynamicCepCoordinatorProvider(
            String operatorName,
            OperatorID operatorID,
            PatternProcessorDiscovererFactory<IN> discovererFactory) {
        this.operatorName = operatorName;
        this.operatorID = operatorID;
        this.discovererFactory = discovererFactory;
    }

    @Override
    public OperatorID getOperatorId() {
        return operatorID;
    }

    @Override
    public OperatorCoordinator create(
            OperatorCoordinator.Context context) {
        return new DynamicCepCoordinator<>(
                operatorName, discovererFactory, context);
    }
}
