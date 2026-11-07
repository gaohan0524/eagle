package com.alarm.eagle.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 运行参数装配器。
 *
 * <p>业务含义：根据运行模式（dev/test/prod）注入各环境的默认参数
 * （Kafka 地址、topic、eagle-api 地址、轮询间隔等），再被命令行传入的参数覆盖，
 * 最终合并成一份 {@link ParameterTool} 供 {@code DetectionApp} 使用。
 *
 * <p>设计目的：把"环境差异"集中到一处，作业代码只依赖统一的参数 key（见 {@link Constant}）。
 */
public class DetectionProperties {
    private static final Logger logger = LoggerFactory.getLogger(DetectionProperties.class);

    private ParameterTool parameter;
    private static ParameterTool params;
    private static String mode = "";

    private static class PropertiesHolder {
        private static final DetectionProperties holder = new DetectionProperties();
    }

    public static DetectionProperties getInstance(ParameterTool parameterTool) {
        params = parameterTool;
        mode = params.get(Constant.FLINK_MODE);
        return PropertiesHolder.holder;
    }

    private DetectionProperties() {
        init();
    }

    private void init() {
        try {
            logger.info("load parameters from system ...");
            Map<String, String> map = new HashMap<>();
            switch (mode) {
                case Constant.MODE_DEV:
                    map.put(Constant.FLINK_MODE, Constant.MODE_DEV);
                    map.put(Constant.API_BASE_URL, "http://localhost:8080/eagle-api");
                    map.put(Constant.CEP_RULE_POLL_INTERVAL_MS, "30000");

                    map.put(Constant.KAFKA_BOOTSTRAP_SERVERS, "localhost:9092");
                    map.put(Constant.KAFKA_GROUP_ID, "eagle-detection-" + Constant.MODE_DEV);
                    map.put(Constant.KAFKA_TOPIC, "eagle-transaction");
                    map.put(Constant.KAFKA_TOPIC_PARALLELISM, "1");
                    map.put(Constant.TRANSACTIONS_OUT_OF_ORDERNESS, "60000");

                    break;
                case Constant.MODE_TEST:
                    // ....
                    break;
            }
            map.put(Constant.FLINK_PARALLELISM, "2");
            map.put(Constant.FLINK_ENABLE_CHECKPOINT, "true");

            Map<String, String> paramsMap = params.toMap();
            map.putAll(paramsMap);
            parameter = ParameterTool.fromMap(map);

        } catch (Exception ex) {
            logger.error("load parameters from system error: " + ex.toString());
        }
    }

    public ParameterTool getParameter() {
        return parameter;
    }

}
