package com.alarm.eagle.config;

/**
 * 配置常量接口。
 *
 * <p>集中管理 Flink 作业运行时所有配置项的 key（由 {@code ParameterTool} 按 key 读取）。
 * 这些 key 会被 {@link DetectionProperties} 填充默认值，并被 {@code DetectionApp} 消费，
 * 是"命令行/环境配置 → 作业运行参数"之间唯一的约定桥梁。
 *
 * <p>业务含义：每个 key 对应一条可外部化配置的运行参数，避免配置散落在代码中。
 */
public interface Constant {

    /** 运行模式 key，取值见 MODE_DEV / MODE_TEST / MODE_PROD */
    String FLINK_MODE = "mode";
    /** 本地开发模式（单并行度，便于本地联调） */
    String MODE_DEV = "dev";
    /** 测试环境模式 */
    String MODE_TEST = "test";
    /** 生产环境模式 */
    String MODE_PROD = "prod";

    /** Flink 全局并行度 key */
    String FLINK_PARALLELISM = "flink.parallelism";
    /** 是否开启 Checkpoint（容错机制）的开关 key */
    String FLINK_ENABLE_CHECKPOINT = "flink.enable.checkpoint";

    /** eagle-api 基础 URL（用于拉取 CEP 规则），例如 {@code http://localhost:8080/eagle-api} */
    String API_BASE_URL = "api.base.url";

    /** CEP 规则轮询间隔（毫秒），控制后台发现规则变更的频率 */
    String CEP_RULE_POLL_INTERVAL_MS = "cep.rule.poll.interval.ms";

    /** Kafka 集群地址（bootstrap.servers） */
    String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    /** Kafka 消费组 id */
    String KAFKA_GROUP_ID = "kafka.group.id";
    /** 交易事件所在的 Kafka topic */
    String KAFKA_TOPIC = "kafka.topic";
    /** Kafka 源并行度 */
    String KAFKA_TOPIC_PARALLELISM = "kafka.topic.parallelism";
    /** Kafka SASL 用户名（可选，启用认证时使用） */
    String KAFKA_SASL_USERNAME = "kafka.sasl.username";
    /** Kafka SASL 密码（可选，启用认证时使用） */
    String KAFKA_SASL_PASSWORD = "kafka.sasl.password";

    /** 事件乱序容忍度（毫秒），作为 {@code WatermarkStrategy.forBoundedOutOfOrderness} 的参数 */
    String TRANSACTIONS_OUT_OF_ORDERNESS = "transaction.out.of.orderness";
}
