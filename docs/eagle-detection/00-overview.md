# eagle-detection 代码精读 · 00 总纲与阅读地图

> 本系列文档把 `eagle-detection` 模块按**学习依赖顺序**拆成 8 个部分，每部分一个文档，逐层深入。
> 所有结论以源码为准，无法确认的业务语义标注【待确认业务】。

---

## 一、为什么要划分

`eagle-detection` 不是一个"平铺"的业务模块，而是**一个自研的 CEP 引擎** + **一个业务作业**的组合。它的代码有三层不同性质：

1. **业务层**：交易事件、作业入口 —— 容易看懂，但看不出引擎精髓；
2. **框架层**：规则抽象、规则序列化、规则发现、JM 协调器 —— 是引擎的"外围器官"；
3. **引擎层**：`DynamicCepOperator` —— 900+ 行，是整个模块的难度巅峰。

如果按包名顺序从 `cep/condition` 开始读，会在没有全局图景的情况下陷入细节。正确方式是**自顶向下、沿数据流走一遍**。

---

## 二、划分结果与阅读顺序

```
                    ┌──────────────────────────────┐
                    │ 01 作业入口与事件模型          │
                    │ DetectionApp / Transaction    │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │ 02 API 门面与规则抽象          │
                    │ DynamicCEP / PatternProcessor │
                    └──────────────┬───────────────┘
                                   │
              ┌────────────────────┴────────────────────┐
              │                                         │
┌─────────────▼──────────────┐        ┌─────────────────▼──────────────┐
│ 03 规则 JSON 模型与序列化    │        │ 04 动态条件引擎                 │
│ GraphSpec / NodeSpec / ... │        │ AviatorCondition               │
└─────────────┬──────────────┘        └─────────────────┬──────────────┘
              │                                         │
              └────────────────────┬────────────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │ 05 规则发现层                  │
                    │ Discoverer 体系                │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │ 06 JM 侧协调器                 │
                    │ DynamicCepCoordinator         │
                    └──────────────┬───────────────┘
                                   │ OperatorEvent
                    ┌──────────────▼───────────────┐
                    │ 07 TM 侧执行引擎（核心）        │
                    │ DynamicCepOperator            │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │ 08 端到端串讲与测试             │
                    │ CepTestDemo / 集成示例          │
                    └──────────────────────────────┘
```

### 各部分的定位与"读完后应能回答"

| 序 | 文档 | 性质 | 读完后应能回答 |
|---|---|---|---|
| 01 | 作业入口与事件模型 | 业务层 | 作业由哪些部件组成？事件字段有哪些？时间语义怎么定？ |
| 02 | API 门面与规则抽象 | 框架层 | `DynamicCEP.dynamicPatterns()` 做了什么？"一条规则"在内存里是什么？ |
| 03 | 规则 JSON 模型与序列化 | 框架层 | 一条 JSON 规则怎么变成 Flink `Pattern`？ |
| 04 | 动态条件引擎 | 框架层 | `"amount > 50000"` 怎么对每条事件求值？ |
| 05 | 规则发现层 | 框架层 | 规则从哪来？怎么知道变了？ |
| 06 | JM 侧协调器 | 框架层 | 全量列表怎么变成增量？怎么下发到 TM？ |
| 07 | TM 侧执行引擎 | **引擎核心** | 多规则怎么并行？状态怎么隔离？热更新怎么做？ |
| 08 | 端到端串讲与测试 | 实践 | 全链路怎么串起来？怎么调试？ |

---

## 三、各部分详细说明

### 01 作业入口与事件模型

**文件**

| 文件 | 行数量级 | 作用 |
|---|---|---|
| `DetectionApp.java` | ~280 | Flink 作业入口，组装全链路 |
| `message/Transaction.java` | ~90 | CEP 处理的事件 DTO |
| `config/Constant.java` | ~30 | 配置 key 常量 |
| `config/DetectionProperties.java` | ~70 | 多环境参数装配 |

**关键问题**：为什么启动时要**同步**拉规则？为什么 `env.execute()` 放在 try-catch 里有隐患？事件时间戳从哪来？

---

### 02 API 门面与规则抽象

**文件**

| 文件 | 作用 |
|---|---|
| `cep/DynamicCEP.java` | 对外唯一 API，组装算子 |
| `cep/PatternWrapper.java` | 暴露 `Pattern` 的 protected 构造器 |
| `cep/processor/PatternProcessor.java` | 规则抽象接口 |
| `cep/processor/DefaultPatternProcessor.java` | 规则默认实现（持 JSON 字符串） |

**关键问题**：为什么规则存 JSON 而不是 `Pattern` 对象？为什么非 KeyedStream 会被 `forceNonParallel()`？

---

### 03 规则 JSON 模型与序列化

**文件**

| 文件 | 作用 |
|---|---|
| `cep/json/CepJsonUtils.java` | JSON ↔ Pattern 双向转换入口 |
| `cep/json/TimeDeserializer.java` | Flink `Time` 的反序列化 |
| `cep/json/spec/GraphSpec.java` | 整条规则的图表示（**核心**） |
| `cep/json/spec/NodeSpec.java` | 单节点还原（**核心**） |
| `cep/json/spec/ConditionSpec.java` + 5 子类 | 条件多态树 |
| `cep/json/spec/QuantifierSpec.java` | 量词 |
| `cep/json/spec/TimesSpec.java` | 次数 |
| `cep/json/spec/WindowSpec.java` | 时间窗口 |
| `cep/json/spec/EdgeSpec.java` | 节点连接边 |
| `cep/json/spec/AfterMatchSkipStrategySpec.java` | 命中后跳过策略 |

**关键问题**：`toPattern()` 如何从入度为 0 的节点串出 Pattern 链？COMPOSITE 与 ATOMIC 节点有何区别？

---

### 04 动态条件引擎

**文件**

| 文件 | 作用 |
|---|---|
| `cep/condition/AviatorCondition.java` | Aviator 表达式条件（**核心**） |

**关键问题**：为什么 `compiledExpression` 要 `transient`？MethodHandle 缓存解决了什么性能问题？Aviator 为什么不能用 `previous`？

---

### 05 规则发现层

**文件**

| 文件 | 作用 |
|---|---|
| `PatternProcessorDiscoverer.java` | 发现器接口 |
| `PatternProcessorDiscovererFactory.java` | 工厂接口 |
| `PatternProcessorManager.java` | 更新回调接口 |
| `PeriodicPatternProcessorDiscoverer.java` | 周期轮询基类（Timer） |
| `ApiPatternProcessorDiscoverer.java` | HTTP 轮询（**生产用**） |
| `ApiPatternProcessorDiscovererFactory.java` | HTTP 工厂 |
| `JDBCPeriodicPatternProcessorDiscoverer.java` | JDBC 轮询（备选） |
| `JDBCPeriodicPatternProcessorDiscovererFactory.java` | JDBC 工厂 |
| `RuleState.java` | 生命周期枚举（未使用） |

**关键问题**：`initialProcessors` 兜底逻辑怎么工作？为什么 `TimerTask` 里必须 catch 所有异常？

---

### 06 JM 侧协调器

**文件**

| 文件 | 作用 |
|---|---|
| `DynamicCepCoordinator.java` | **核心**：发现→增量→下发→checkpoint |
| `DynamicCepCoordinatorProvider.java` | 协调器工厂 Provider |
| `DynamicCepOperatorFactory.java` | 算子工厂 + 事件通道注册 |
| `PatternProcessorChangeEvent.java` | 增量变更事件 |
| `RuleLifecycleEvent.java` | 生命周期事件 |

**关键问题**：`computeDelta` 如何区分 added/removed/updated？为什么 TM 恢复时发全量而不是增量？

---

### 07 TM 侧执行引擎

`DynamicCepOperator.java`（900+ 行）建议再拆 4 个子篇精读：

| 子篇 | 内容 | 关键点 |
|---|---|---|
| 07a 数据结构与生命周期 | 字段、`RuleContext`、`initializeState`/`open`/`close` | 每规则一套状态 |
| 07b 事件处理与时间语义 | `processElement`、`processTimedEvents`、`processEvent`、`advanceTime` | 广播、缓冲、回放、超时 |
| 07c 规则热更新 | `handleOperatorEvent`、全量/增量、`createRuleContext` | 复用/新建/下线、原子替换 |
| 07d 生命周期与内部类 | `handleLifecycleEvent`、`RuleKey`、`ContextFunctionImpl`、`TimerServiceImpl` | 暂停/排空/删除 |

**关键问题**：状态名为什么带 `{id}-{version}`？`pendingStateClear` 为什么要延迟？全量替换为什么先建新 Map？

---

### 08 端到端串讲与测试

**文件**

| 文件 | 作用 |
|---|---|
| `CepTestDemo.java` | 自包含测试（4 条规则 + 场景） |
| `CepApiIntegrationDemo.java` | 与 eagle-api 联调 |
| `DemoCepOutputFunction.java` | 输出函数 |
| `UserBehaviorEvent.java` | 测试事件类 |

**关键问题**：负向用例（Eve/Frank）为什么不该命中？怎么调试"事件到了但没匹配"？

---

## 四、阅读前置知识

建议先掌握以下 Flink 概念，否则第 07 部分会吃力：

| 知识点 | 需要到什么程度 |
|---|---|
| Flink CEP `Pattern` API | 会写 `begin/next/followedBy/where/times/within` |
| 连续性策略 | 分清 `STRICT` / `SKIP_TILL_NEXT` / `SKIP_TILL_ANY` |
| NFA | 知道 Pattern 会被编译成状态机 |
| `KeyedState` | 了解 `ValueState` / `MapState` 的 key 语义 |
| `Watermark` | 理解事件时间与乱序容忍 |
| `OperatorCoordinator` | 知道 JM 侧协调器与 OperatorEvent 机制（第 06 部分会讲） |

> 完整的 Flink CEP 基础讲解见 [`../eagle-detection-flink-cep-study.md`](../eagle-detection-flink-cep-study.md) 第一部分。

---

## 五、如何使用本系列文档

1. **按顺序读**：01→08，不要跳。每篇都建立在前一篇之上。
2. **对照源码**：每篇都会给出关键代码片段与类/方法定位，边读边打开源码。
3. **做自测**：每篇末尾有"自测问题"，答不上来就回读。
4. **动手验证**：第 08 部分提供实验清单。

---

## 六、文档索引

| 文档 | 状态 | 内容 |
|---|---|---|
| `00-overview.md` | ✅ | 总纲与阅读地图 |
| `01-entry-and-event.md` | ✅ | 作业入口与事件模型 |
| `02-api-and-rule-abstraction.md` | ✅ | API 门面与规则抽象 |
| `03-rule-json-serialization.md` | ✅ | 规则 JSON 模型与序列化 |
| `04-dynamic-condition.md` | ✅ | 动态条件引擎 |
| `05-rule-discovery.md` | ✅ | 规则发现层 |
| `06-jm-coordinator.md` | ✅ | JM 侧协调器 |
| `07a-operator-data-structure.md` | ✅ | 执行引擎：数据结构与生命周期 |
| `07b-operator-event-processing.md` | ✅ | 执行引擎：事件处理与时间语义 |
| `07c-operator-hot-swap.md` | ✅ | 执行引擎：规则热更新 |
| `07d-operator-lifecycle-inner-classes.md` | ✅ | 执行引擎：生命周期与内部类 |
| `08-end-to-end-and-testing.md` | ✅ | 端到端串讲与测试 |
