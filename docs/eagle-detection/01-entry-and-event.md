# eagle-detection 代码精读 · 01 作业入口与事件模型

> 本篇精读 4 个文件：`DetectionApp`、`Transaction`、`Constant`、`DetectionProperties`。
> **读完后应能回答**：这个 Flink 作业由哪些部件组成？事件长什么样？时间语义怎么定？配置从哪来？

---

## 1. 本部分在整体中的位置

```
【本篇范围】
DetectionApp  ── 组装 ──►  Flink 环境
     │                      Kafka 源
     │                      Transaction（事件模型）
     │                      规则预加载
     ▼
DynamicCEP.dynamicPatterns(...)   ← 下一篇（02）
     │
     ▼
DynamicCepOperator                ← 第 07 篇
```

`DetectionApp` 是**唯一的主类**，它不实现任何 CEP 逻辑，只做"装配工"：把 Flink 环境、Kafka 源、规则发现器、CEP 引擎、输出串起来。理解它的关键不是读代码，而是**理解每一步为什么这么写**。

---

## 2. `DetectionApp.java` 精读

### 2.1 类的定位

```java
public class DetectionApp {
    private static final Logger logger = LoggerFactory.getLogger(DetectionApp.class);
    private static final ObjectMapper mapper = new ObjectMapper();
```

| 成员 | 作用 |
|---|---|
| `logger` | 日志 |
| `mapper` | Jackson `ObjectMapper`，仅用于解析 eagle-api 返回的规则响应 |

类上没有 `@Slf4j`（虽然依赖里有 Lombok），是手写 logger——与模块整体风格一致。

### 2.2 `main()` 六步逐段讲解

```java
public static void main(String[] args) {
    try {
        ParameterTool params = ParameterTool.fromArgs(args);
        ParameterTool parameter = DetectionProperties.getInstance(params).getParameter();
        showConf(parameter);

        // ---- 1. 创建 Flink 环境 ----
        StreamExecutionEnvironment env = getStreamExecutionEnvironment(parameter);

        // ---- 2. Kafka 数据源 ----
        DataStream<Transaction> transactions = getTransactionStream(env, parameter);

        // ---- 3. 启动时同步加载 CEP 规则 ----
        String apiBaseUrl = parameter.get(Constant.API_BASE_URL);
        List<PatternProcessor<Transaction>> initialRules = fetchRulesFromApi(apiBaseUrl);
        logger.info("Loaded {} initial CEP rules from API", initialRules.size());

        // ---- 4. 创建 DiscovererFactory ----
        long pollIntervalMs = parameter.getLong(Constant.CEP_RULE_POLL_INTERVAL_MS, 30000L);
        PatternProcessorDiscovererFactory<Transaction> discovererFactory =
                new ApiPatternProcessorDiscovererFactory<>(apiBaseUrl, initialRules, pollIntervalMs);

        // ---- 5. Dynamic CEP ----
        SingleOutputStreamOperator<String> cepResults = DynamicCEP.dynamicPatterns(
                transactions, discovererFactory, false,  // EventTime
                TypeInformation.of(String.class));

        // ---- 6. 输出 ----
        cepResults.print("CEP-ALERT").setParallelism(1).name("CEP Alert STDOUT");

        env.getConfig().setGlobalJobParameters(parameter);
        env.execute("eagle-detection");

    } catch (Exception e) {
        logger.error("DetectionApp error: ", e);
    }
}
```

**逐行要点：**

| 步骤 | 代码 | 为什么这么写 |
|---|---|---|
| 参数 | `ParameterTool.fromArgs(args)` | 支持 `--key value` 命令行覆盖 |
| 参数合并 | `DetectionProperties.getInstance(params)` | 环境默认值 + 命令行覆盖（见 2.6） |
| 环境 | `getStreamExecutionEnvironment` | dev/prod 分支 + 可选 checkpoint |
| 数据源 | `getTransactionStream` | Kafka + CSV 解析 + 水位 |
| **规则预加载** | `fetchRulesFromApi` | **同步** HTTP 拉取，保证事件到达前规则已就绪 |
| 工厂 | `ApiPatternProcessorDiscovererFactory` | 携带 `initialRules` + 轮询间隔 |
| 引擎 | `DynamicCEP.dynamicPatterns(..., false, ...)` | `false` = EventTime 语义 |
| 输出 | `print("CEP-ALERT")` | 当前仅 STDOUT |
| 执行 | `env.execute("eagle-detection")` | 作业名称 |

**两个必须注意的设计细节：**

1. **规则预加载是同步的、在 `env.execute()` 之前**。如果只依赖后台轮询，第一批事件到达时可能还没有规则，事件会被 `DynamicCepOperator` 直接丢弃（见第 07 篇）。
2. **`env.execute()` 被包在 `try-catch` 里**。`execute()` 是阻塞的，作业运行期异常会被这个 `catch` 吞掉、只打日志，可能导致作业"静默失败"而外部无感知。这是**已知隐患**，生产环境应让其抛出。

### 2.3 `getStreamExecutionEnvironment()`：环境与 checkpoint

```java
private static StreamExecutionEnvironment getStreamExecutionEnvironment(ParameterTool parameter) {
    StreamExecutionEnvironment env;
    int globalParallelism = parameter.getInt(Constant.FLINK_PARALLELISM);

    if (parameter.get(Constant.FLINK_MODE).equals(Constant.MODE_DEV)) {
        env = StreamExecutionEnvironment.createLocalEnvironment();
        globalParallelism = 1;          // dev 强制单并行度
    } else {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }
    env.setParallelism(globalParallelism);

    boolean enableCheckpoint = parameter.getBoolean(Constant.FLINK_ENABLE_CHECKPOINT, false);
    if (enableCheckpoint) {
        env.enableCheckpointing(60000L);
        CheckpointConfig config = env.getCheckpointConfig();
        config.setMinPauseBetweenCheckpoints(30000L);
        config.setCheckpointTimeout(10000L);
        config.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
    }
    return env;
}
```

**要点：**

- **dev 模式固定并行度 1**：本地环境便于调试、日志不交叉。
- **checkpoint 参数**：间隔 60s、最小间隔 30s、超时 10s、取消时删除外部 checkpoint。
- 注意 `setCheckpointTimeout(10000L)` 比 `setMinPauseBetweenCheckpoints(30000L)` 短，意味着 checkpoint 超时时间只有 10s，在状态较大时可能频繁超时——**调优时需关注**。
- prod 分支如果没配 `flink.parallelism`，`getInt` 会抛异常；默认值由 `DetectionProperties` 提供（`"2"`）。

### 2.4 `getTransactionStream()`：Kafka 源 + 水位

```java
private static DataStream<Transaction> getTransactionStream(
        StreamExecutionEnvironment env, ParameterTool parameter) {

    String bootstrapServers = parameter.get(Constant.KAFKA_BOOTSTRAP_SERVERS);
    String groupId          = parameter.get(Constant.KAFKA_GROUP_ID);
    String topic            = parameter.get(Constant.KAFKA_TOPIC);
    int parallelism         = parameter.getInt(Constant.KAFKA_TOPIC_PARALLELISM);
    int orderness           = parameter.getInt(Constant.TRANSACTIONS_OUT_OF_ORDERNESS);
    String username         = parameter.get(Constant.KAFKA_SASL_USERNAME, "");
    String password         = parameter.get(Constant.KAFKA_SASL_PASSWORD, "");

    // 1) 可选 SASL 认证
    Properties props = new Properties();
    if (!username.isEmpty() && !password.isEmpty()) {
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(...));
    }

    // 2) KafkaSource
    KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(topic)
            .setGroupId(groupId)
            .setProperties(props)
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

    // 3) 源 → 解析 → 水位
    return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka-" + topic)
            .setParallelism(parallelism)
            .map(Transaction::fromString)
            .name("Transaction Parser")
            .assignTimestampsAndWatermarks(
                    WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(
                                    Duration.ofMillis(orderness))
                            .withTimestampAssigner((txn, ts) -> txn.getEventTime()));
}
```

**逐段解释：**

| 段落 | 作用 | 注意点 |
|---|---|---|
| SASL | 可选认证 | 只有用户名和密码都非空才启用 |
| `KafkaSource` | 新版 Kafka 连接器 | `committedOffsets(EARLIEST)`：无提交位点时从最早开始 |
| `SimpleStringSchema` | 值反序列化为 String | 后续手动 CSV 解析 |
| `noWatermarks()` | 源处不生成水位 | **正确**：先解析出 `eventTime` 再生成水位 |
| `map(Transaction::fromString)` | CSV → 对象 | 解析失败会抛异常，可能中断作业（见 3.3） |
| `forBoundedOutOfOrderness(orderness)` | 乱序容忍 | dev 默认 60000ms |
| `withTimestampAssigner` | 提取事件时间 | `txn.getEventTime()` |

**关键时序理解**：源先 `noWatermarks`，`map` 解析后再 `assignTimestampsAndWatermarks`——因为源里拿到的是字符串，还没有 `eventTime` 字段。这是 Flink 中"解析后再打时间戳"的标准写法。

> ⚠️ **水位生成位置与 CEP 并行度的关系**：`DynamicCEP.dynamicPatterns` 对非 KeyedStream 会 `keyBy(NullByteKeySelector)` + `forceNonParallel()`（第 02 篇详解），因此 CEP 算子是单并行度。而 Kafka 源可以多并行度（`kafka.topic.parallelism`），事件会经 shuffle 汇聚到单并行度的 CEP 算子。

### 2.5 `fetchRulesFromApi()`：启动时同步加载

```java
private static List<PatternProcessor<Transaction>> fetchRulesFromApi(String apiBaseUrl) {
    List<PatternProcessor<Transaction>> rules = new ArrayList<>();
    try {
        String json = httpGet(apiBaseUrl + "/cep/rules");
        if (json == null || json.isEmpty()) {
            logger.warn("Empty response from CEP rules API, starting with no rules");
            return rules;
        }

        JsonNode root = mapper.readTree(json);
        JsonNode dataNode = root.has("data") ? root.get("data") : root;
        if (!dataNode.isArray()) return rules;

        for (JsonNode item : dataNode) {
            String ruleId    = item.get("ruleId").asText();
            int version      = item.get("version").asInt();
            String patternStr = item.get("pattern").asText();

            rules.add(new DefaultPatternProcessor<>(
                    ruleId, version, patternStr,
                    new DetectionAlertFunction(ruleId, version)));
            logger.info("  Loaded rule: {} (v{})", ruleId, version);
        }
    } catch (Exception e) {
        logger.error("Failed to fetch CEP rules from API, starting with no rules: {}", e.getMessage());
    }
    return rules;
}
```

**要点：**

1. **响应结构**：兼容 `{ "data": [...] }` 包装和直接数组两种形式。
2. **字段名是 `ruleId`**（不是 `id`）——与 `ApiPatternProcessorDiscoverer` 保持一致。
3. **每条规则都绑定了一个 `DetectionAlertFunction`**：这是启动期规则与后台轮询规则的关键差异（后者 `function` 为 `null`，是已知 NPE 隐患，见第 05 篇）。
4. **失败不阻断启动**：任何异常都只打日志并返回空列表，作业会以"无规则"状态启动。

### 2.6 `DetectionProperties`：参数装配（精读）

```java
public class DetectionProperties {
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

    private DetectionProperties() { init(); }

    private void init() {
        try {
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
            map.putAll(paramsMap);               // 命令行覆盖默认值

            parameter = ParameterTool.fromMap(map);
        } catch (Exception ex) {
            logger.error("load parameters from system error: " + ex.toString());
        }
    }

    public ParameterTool getParameter() { return parameter; }
}
```

**设计意图：**

- **单例 + 静态字段**：`params`/`mode` 在 `getInstance` 里先赋值，再触发 `PropertiesHolder` 初始化（懒加载）。
- **优先级**：环境默认值 < 命令行参数（`map.putAll(paramsMap)` 覆盖）。
- **dev 默认值**：本地 Kafka、本地 API、30s 轮询、乱序 60s。

**潜在问题（阅读时要意识到）：**

1. **单例只初始化一次**。`PropertiesHolder.holder` 是静态 final，第二次 `getInstance(newParams)` 不会重新执行 `init()`，新参数不生效。当前作业只调用一次，所以没出问题。
2. **`MODE_TEST` 分支为空**，test 模式没有默认值，全靠命令行。
3. **`FLINK_PARALLELISM` / `FLINK_ENABLE_CHECKPOINT` 的默认值在 switch 之外**，所以所有模式都生效。

### 2.7 `showConf()` 与 `httpGet()`

```java
private static void showConf(ParameterTool parameter) {
    logger.info("Show {} config parameters", parameter.getNumberOfParameters());
    Configuration configuration = parameter.getConfiguration();
    for (String key : configuration.toMap().keySet()) {
        logger.info("  {} = {}", key, configuration.getString(key, ""));
    }
}
```

- 启动时打印全部配置，便于排查环境问题。
- ⚠️ **会把 `kafka.sasl.password` 等敏感信息打到日志**，生产环境需脱敏。

```java
private static String httpGet(String urlStr) {
    URL url = new URL(urlStr);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);
    conn.setRequestProperty("Accept", "application/json");
    if (conn.getResponseCode() != 200) return null;
    // ... 读取响应体
    conn.disconnect();
    return sb.toString();
}
```

- 连接/读取超时各 5s，失败返回 `null`（而非抛异常）。
- 没有重试机制，依赖下一次轮询。

### 2.8 内部类 `DetectionAlertFunction`

```java
public static class DetectionAlertFunction extends PatternProcessFunction<Transaction, String> {
    private final String ruleId;
    private final int version;

    public DetectionAlertFunction(String ruleId, int version) { ... }

    @Override
    public void processMatch(
            Map<String, List<Transaction>> match,
            Context ctx, Collector<String> out) {
        StringBuilder sb = new StringBuilder();
        sb.append("[ALERT] rule=").append(ruleId).append(" v").append(version).append(" | ");

        for (Map.Entry<String, List<Transaction>> entry : match.entrySet()) {
            sb.append(entry.getKey()).append("=[");
            for (Transaction txn : entry.getValue()) {
                sb.append("txnId=").append(txn.getTransactionId())
                  .append(",payer=").append(txn.getPayerId())
                  .append(",beneficiary=").append(txn.getBeneficiaryId())
                  .append(",amount=").append(txn.getPaymentAmount())
                  .append("; ");
            }
            sb.append("] ");
        }
        out.collect(sb.toString());
    }
}
```

**要点：**

- `match` 的 key 是 **Pattern 节点名**（规则 JSON 里的 `node.name`），value 是该节点命中的事件列表。
- 构造函数签名为 `(String ruleId, int version)`，与 JDBC Discoverer 的反射实例化约定一致。
- 只做格式化，不做业务判断（判断已在 NFA 阶段完成）。
- 没有实现 `TimedOutPartialMatchHandler`，因此**超时部分匹配不会输出**。

### 2.9 本类设计要点与易错点汇总

| 要点 | 说明 |
|---|---|
| 规则预加载 | 同步、在 `execute()` 前，保证首批事件有规则 |
| 时间语义 | `false` = EventTime，水位在 `map` 之后生成 |
| 并行度 | dev 强制 1；CEP 算子自身还会 `forceNonParallel()` |
| 输出 | 仅 STDOUT，无 Kafka sink【待确认业务】 |
| 异常处理 | `env.execute()` 被 catch 吞掉，有静默失败风险 |
| 敏感信息 | `showConf` 会打印密码 |

---

## 3. `Transaction.java` 精读

### 3.1 类的定位

CEP 算子处理的事件类型（泛型 `IN`）。它只需要满足两个条件：

1. **可序列化**（`implements Serializable`）——Flink 需要在网络上传输、状态后端存储；
2. **有 getter**——Aviator 表达式通过 getter 访问字段。

### 3.2 字段与业务含义

| 字段 | 类型 | 业务含义 | 在 CEP 中的作用 |
|---|---|---|---|
| `transactionId` | `long` | 交易唯一 ID | 告警时定位交易 |
| `eventTime` | `long` | 交易发生时间（epoch ms） | **水位与窗口的唯一依据** |
| `payerId` | `long` | 付款方 ID | 可用于按账户维度关联 |
| `beneficiaryId` | `long` | 收款方 ID | 交易对手方 |
| `paymentAmount` | `BigDecimal` | 交易金额 | 金额阈值条件的目标字段 |
| `paymentType` | `String` | 支付类型 | 取值枚举源码未定义【待确认业务】 |
| `ingestionTimestamp` | `Long` | 接入时间戳 | 是否参与匹配【待确认业务】 |

**为什么金额用 `BigDecimal`？** 避免浮点精度丢失，金融场景必须。

**Aviator 访问规则**：表达式里写 `paymentAmount > 50000`，`AviatorCondition` 会查找 `getPaymentAmount()`。字段名与 getter 的对应关系是**驼峰映射**。

### 3.3 `fromString()`：CSV 解析

```java
public static Transaction fromString(String line) {
    List<String> tokens = Arrays.asList(line.split(","));
    if (tokens.size() != 7) {
        throw new RuntimeException("Invalid transaction: " + line
                + ". Required 7 fields, found " + tokens.size());
    }

    Transaction transaction = new Transaction();
    try {
        Iterator<String> iter = tokens.iterator();
        transaction.transactionId    = Long.parseLong(iter.next());
        transaction.eventTime        = ZonedDateTime.parse(iter.next(), timeFormatter)
                                               .toInstant().toEpochMilli();
        transaction.payerId          = Long.parseLong(iter.next());
        transaction.beneficiaryId    = Long.parseLong(iter.next());
        transaction.paymentType      = iter.next();
        transaction.paymentAmount    = new BigDecimal(iter.next());
        transaction.ingestionTimestamp = Long.parseLong(iter.next());
    } catch (NumberFormatException nfe) {
        throw new RuntimeException("Invalid record: " + line, nfe);
    }
    return transaction;
}
```

**输入格式（7 列，逗号分隔）：**

```
transactionId,eventTime,payerId,beneficiaryId,paymentType,paymentAmount,ingestionTimestamp
```

**时间解析：**

```java
private static final transient DateTimeFormatter timeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .withLocale(Locale.US).withZone(ZoneOffset.UTC);
```

- 格式：`yyyy-MM-dd HH:mm:ss`
- **UTC 时区**：如果上游数据是本地时间，会产生时区偏移，需注意。
- `transient`：`DateTimeFormatter` 不可序列化，但它是 `static` 的，Flink 序列化实例时不涉及。

**异常处理：**

- 字段数不对 → `RuntimeException`
- 数字解析失败 → `RuntimeException` 包装
- **异常会中断整个作业**（`map` 算子中未捕获）。生产环境通常应改为跳过坏数据 + 计数，而不是让作业失败。

### 3.4 序列化

```java
private static final long serialVersionUID = 1L;
```

Flink 用 `TypeExtractor` 为 `Transaction` 生成序列化器（POJO 序列化）。**作为 POJO 的条件**：public 无参构造、字段有 getter/setter、非 static/transient。本类满足（`public Transaction() {}`）。

> 这也是为什么必须有 `public Transaction() {}`——Flink POJO 序列化器需要无参构造。

---

## 4. `Constant.java` 精读

纯常量接口（`interface` 的字段隐式 `public static final`）。设计目的：**配置 key 的唯一约定**。

| 常量 | key | 用途 |
|---|---|---|
| `FLINK_MODE` | `mode` | 运行模式 |
| `MODE_DEV/TEST/PROD` | `dev/test/prod` | 模式取值 |
| `FLINK_PARALLELISM` | `flink.parallelism` | 全局并行度 |
| `FLINK_ENABLE_CHECKPOINT` | `flink.enable.checkpoint` | checkpoint 开关 |
| `API_BASE_URL` | `api.base.url` | eagle-api 地址 |
| `CEP_RULE_POLL_INTERVAL_MS` | `cep.rule.poll.interval.ms` | 规则轮询间隔 |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka.bootstrap.servers` | Kafka 地址 |
| `KAFKA_GROUP_ID` | `kafka.group.id` | 消费组 |
| `KAFKA_TOPIC` | `kafka.topic` | 交易 topic |
| `KAFKA_TOPIC_PARALLELISM` | `kafka.topic.parallelism` | 源并行度 |
| `KAFKA_SASL_USERNAME/PASSWORD` | `kafka.sasl.*` | SASL 认证 |
| `TRANSACTIONS_OUT_OF_ORDERNESS` | `transaction.out.of.orderness` | 乱序容忍（ms） |

> 注意拼写 `ORDERNESS` 不是 `ORDERLINESS`，改 key 会破坏现有配置兼容性。

---

## 5. 本篇小结

### 5.1 一句话总结

`DetectionApp` 是一个"装配工"：**同步预加载规则 → 建 Kafka EventTime 源 → 交给 DynamicCEP → 打印告警**；`Transaction` 是 CEP 的输入事件，其 `eventTime` 决定一切时间语义。

### 5.2 自测问题

1. 为什么规则要在 `env.execute()` 之前同步加载一次？
2. 为什么 Kafka 源处用 `noWatermarks()`，而在 `map` 之后才生成水位？
3. `DetectionAlertFunction` 里 `match` 的 key 和 value 分别是什么？
4. `DetectionProperties` 第二次调用 `getInstance(newParams)` 会发生什么？
5. 一条格式错误的 CSV 会导致什么后果？
6. `paymentAmount` 为什么用 `BigDecimal` 而不是 `double`？

### 5.3 下一篇

→ `02-api-and-rule-abstraction.md`：`DynamicCEP.dynamicPatterns()` 到底做了什么，"一条规则"在内存里由哪些部分组成。
