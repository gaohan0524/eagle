# eagle-detection 代码精读 · 03 规则 JSON 模型与序列化

> 本篇精读 `cep/json/` 与 `cep/json/spec/` 共 15 个类。
> **读完后应能回答**：一条 JSON 规则如何还原成 Flink `Pattern` 链？图结构、条件树、量词分别怎么映射？

---

## 1. 本部分在整体中的位置

```
【第 02 篇】DefaultPatternProcessor.getPattern()
        │  patternStr
        ▼
【本篇】CepJsonUtils.convertJSONStringToPattern()
        │  Jackson 反序列化
        ▼
      GraphSpec (POJO)
        │  GraphSpec.toPattern()   ← 核心
        ▼
      Flink Pattern 链
        │
        ▼
【第 07 篇】NFACompiler.compileFactory()
```

本篇回答一个核心问题：**"规则定义（数据）"如何变成"可执行模式（代码对象）"**。

---

## 2. `CepJsonUtils` 精读

### 2.1 配置

```java
private static final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new SimpleModule()
                .addDeserializer(Time.class, new TimeDeserializer()))
        .enable(SerializationFeature.INDENT_OUTPUT);
```

| 配置 | 作用 |
|---|---|
| `registerModule(...TimeDeserializer)` | Flink `Time` 不是 Jackson 原生类型，需自定义反序列化 |
| `INDENT_OUTPUT` | 输出可读 JSON（调试用） |

**为什么不用全局静态 `ObjectMapper` 的默认行为？** `Time` 是 Flink 的类，字段私有、无无参构造，Jackson 无法默认反序列化。

### 2.2 四个转换方法

| 方法 | 方向 | 用途 |
|---|---|---|
| `convertPatternToJSONString(Pattern)` | Pattern → JSON | 调试、导出规则 |
| `convertJSONStringToPattern(String, ClassLoader)` | JSON → Pattern | **规则加载/热更新核心** |
| `convertJSONStringToGraphSpec(String)` | JSON → GraphSpec | 只解析不还原，便于检查 |
| `convertGraphSpecToJSONString(GraphSpec)` | GraphSpec → JSON | 序列化 |

核心方法：

```java
public static <T> Pattern<T, ?> convertJSONStringToPattern(
        String jsonString, ClassLoader classLoader) throws Exception {
    if (classLoader == null) {
        classLoader = Thread.currentThread().getContextClassLoader();
    }
    GraphSpec graphSpec = objectMapper.readValue(jsonString, GraphSpec.class);
    return (Pattern<T, ?>) graphSpec.toPattern(classLoader);
}
```

**两个细节：**
1. `classLoader == null` 时降级用线程上下文 ClassLoader（但 Flink 中应始终传 user code ClassLoader）。
2. 反序列化目标类型是 `GraphSpec`，它本身 `extends NodeSpec`，所以根节点也是图。

---

## 3. `TimeDeserializer` 精读

```java
public class TimeDeserializer extends JsonDeserializer<Time> {
    @Override
    public Time deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
        if (p.currentToken() == JsonToken.VALUE_NULL) return null;

        String unit = null;
        long size = 0;
        while (p.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = p.currentName();
            p.nextToken();
            if ("unit".equals(fieldName)) unit = p.getValueAsString();
            else if ("size".equals(fieldName)) size = p.getValueAsLong();
        }
        if (unit == null) return null;
        return Time.of(size, TimeUnit.valueOf(unit));
    }
}
```

- 输入格式：`{"unit":"MINUTES","size":15}` 或 `null`。
- 手写 token 循环，而不是绑定 POJO——因为 `Time` 没有合适的构造器。
- `TimeUnit.valueOf(unit)`：unit 必须是 `DAYS/HOURS/MINUTES/SECONDS/MILLISECONDS`。

---

## 4. `GraphSpec` 精读（核心）

### 4.1 结构

```java
public class GraphSpec extends NodeSpec {
    private final int version;
    private final List<NodeSpec> nodes;      // 节点
    private final List<EdgeSpec> edges;      // 边
    private final WindowSpec window;         // 全局窗口
    private final AfterMatchSkipStrategySpec afterMatchStrategy;
}
```

`GraphSpec` **继承 `NodeSpec`**，所以它本身也能作为 `nodes` 里的一个"复合节点"（COMPOSITE），支持嵌套子图。

### 4.2 `toPattern()`：JSON → Pattern（重点逐段）

```java
public Pattern<?, ?> toPattern(ClassLoader classLoader) throws Exception {
    if (nodes.isEmpty()) throw new IllegalStateException("GraphSpec has no nodes.");
```

**① 建索引**

```java
Map<String, NodeSpec> nodeCache = new HashMap<>();
for (NodeSpec ns : nodes) nodeCache.put(ns.getName(), ns);

Map<String, EdgeSpec> edgeCache = new HashMap<>();
for (EdgeSpec es : edges) edgeCache.put(es.getSource(), es);
```

- `nodeCache`：节点名 → 节点。
- `edgeCache`：**源节点名 → 边**。注意：同一个 source 有多条边时，**只保留最后一条**——所以当前实现只支持**线性链**，不支持分支。

**② 找起点**

```java
String beginNodeName = findBeginPatternName();
```

```java
private String findBeginPatternName() {
    Set<String> nodeNames = new HashSet<>();
    for (NodeSpec node : nodes) nodeNames.add(node.getName());

    for (EdgeSpec edge : edges) nodeNames.remove(edge.getTarget());

    if (nodeNames.size() != 1) {
        throw new IllegalStateException(
                "There must be exactly one begin node (indegree=0), but found " + nodeNames.size());
    }
    return nodeNames.iterator().next();
}
```

算法：所有节点名 - 所有边的 target = **入度为 0 的节点**，要求恰好 1 个。这保证是单链（有环/分支会报错）。

**③ 沿边串链**

```java
Pattern<?, ?> prevPattern = null;
String prevNodeName = null;
String currentNodeName = beginNodeName;

while (currentNodeName != null) {
    NodeSpec currentNodeSpec = nodeCache.get(currentNodeName);
    if (currentNodeSpec == null) throw new IllegalStateException(...);

    EdgeSpec edgeToCurrentNode = edgeCache.get(prevNodeName);

    Quantifier.ConsumingStrategy edgeType = prevNodeName == null
            ? Quantifier.ConsumingStrategy.STRICT
            : edgeToCurrentNode.getType();

    Pattern<?, ?> currentPattern = currentNodeSpec.toPattern(
            prevPattern, strategy, edgeType, classLoader);
    ...
    prevPattern = currentPattern;
    prevNodeName = currentNodeName;
    currentNodeName = edgeCache.get(currentNodeName) != null
            ? edgeCache.get(currentNodeName).getTarget()
            : null;
}
```

**关键点：**

| 变量 | 含义 |
|---|---|
| `prevPattern` | 已构建的 Pattern 链尾节点 |
| `edgeType` | 当前节点与前一节点的连接策略；**首节点固定 STRICT** |
| `edgeCache.get(prevNodeName)` | 取出"从上一节点出发"的边 |

首节点的 `edgeType` 用 `STRICT` 是因为它没有前驱；`NodeSpec.toPattern` 内部会用 `PatternWrapper` 构造，`consumingStrategy` 对首节点无实际意义。

**④ 处理复合节点（COMPOSITE）**

```java
if (currentNodeSpec instanceof GraphSpec
        && !(currentNodeSpec instanceof GraphSpec && prevNodeName == null)) {
    if (prevPattern != null) {
        currentPattern = buildGroupConnection(edgeType, currentPattern, prevPattern);
    }
}
```

这段代码写得比较绕，等价于：

```java
if (currentNodeSpec instanceof GraphSpec && prevNodeName != null && prevPattern != null) {
    currentPattern = buildGroupConnection(edgeType, currentPattern, prevPattern);
}
```

即：嵌套子图且**不是首节点**时，用边类型把子图与前一节点连接。

```java
private Pattern<?, ?> buildGroupConnection(ConsumingStrategy strategy,
                                           Pattern<?, ?> currentPattern,
                                           Pattern<?, ?> prevPattern) {
    switch (strategy) {
        case STRICT:        return rawPrev.next(rawCurr);
        case SKIP_TILL_NEXT: return rawPrev.followedBy(rawCurr);
        case SKIP_TILL_ANY:  return rawPrev.followedByAny(rawCurr);
        default:             return rawPrev.followedBy(rawCurr);
    }
}
```

**⑤ 设置全局窗口**

```java
if (window != null && prevPattern != null) {
    prevPattern.within(window.getTime(), window.getType());
}
```

`within(Time, WithinType)` 是 Flink 1.17 的新重载，可指定 `FIRST_AND_LAST` / `PREVIOUS_AND_CURRENT`。

### 4.3 `fromPattern()`：Pattern → JSON

```java
public static GraphSpec fromPattern(Pattern<?, ?> pattern) {
    List<NodeSpec> nodeList = new ArrayList<>();
    List<EdgeSpec> edgeList = new ArrayList<>();

    Pattern<?, ?> current = pattern;
    while (current != null) {
        if (current instanceof GroupPattern) {
            nodeList.add(GraphSpec.fromPattern(((GroupPattern) current).getRawPattern()));
        } else {
            nodeList.add(NodeSpec.fromPattern(current));
        }

        if (current.getPrevious() != null) {
            edgeList.add(new EdgeSpec(
                    resolveName(current.getPrevious()),
                    resolveName(current),
                    current.getQuantifier().getConsumingStrategy()));
        }
        current = current.getPrevious();
    }

    Collections.reverse(nodeList);
    Collections.reverse(edgeList);

    // 收集窗口
    WindowSpec winSpec = null;
    if (pattern.getWindowTime(WithinType.FIRST_AND_LAST) != null) {
        winSpec = WindowSpec.fromWindowTime(WithinType.FIRST_AND_LAST, ...);
    } else if (pattern.getWindowTime(WithinType.PREVIOUS_AND_CURRENT) != null) {
        winSpec = WindowSpec.fromWindowTime(WithinType.PREVIOUS_AND_CURRENT, ...);
    }

    QuantifierSpec q = new QuantifierSpec(
            pattern.getQuantifier(), pattern.getTimes(), pattern.getUntilCondition());
    AfterMatchSkipStrategySpec ams =
            AfterMatchSkipStrategySpec.fromAfterMatchSkipStrategy(
                    pattern.getAfterMatchSkipStrategy());

    return new GraphSpec(resolveName(pattern), q, null,
            nodeList, edgeList, winSpec, ams);
}
```

**要点：**

1. Pattern 是**链表**（`getPrevious()`），从尾往头遍历，最后 `reverse`。
2. `GroupPattern` 递归成子 GraphSpec。
3. 边的 `type` 取自**当前节点的 quantifier 的 consumingStrategy**（Flink 内部就是这么存的）。
4. 窗口从 Pattern 的 `getWindowTime(WithinType)` 提取。

### 4.4 `resolveName()`

```java
private static String resolveName(Pattern<?, ?> pattern) {
    if (pattern instanceof GroupPattern) {
        return ((GroupPattern) pattern).getRawPattern().getName();
    }
    return pattern.getName();
}
```

`GroupPattern` 的名字在其内部 raw pattern 上，需要解包。

---

## 5. `NodeSpec` 精读

### 5.1 结构

```java
public class NodeSpec {
    private final String name;
    private final QuantifierSpec quantifier;
    private final ConditionSpec condition;
    private final PatternNodeType type;   // ATOMIC / COMPOSITE
}
```

Jackson 多态：`ATOMIC` → `NodeSpec`，`COMPOSITE` → `GraphSpec`。

### 5.2 `toPattern()` 逐段

```java
public Pattern<?, ?> toPattern(Pattern<?, ?> previous,
                               AfterMatchSkipStrategy afterMatchSkipStrategy,
                               Quantifier.ConsumingStrategy consumingStrategy,
                               ClassLoader classLoader) throws Exception {

    // ① 复合节点委托给子图
    if (this instanceof GraphSpec) {
        return ((GraphSpec) this).toPattern(classLoader);
    }

    // ② 用 PatternWrapper 绕过 protected 构造器
    Pattern pattern = new PatternWrapper(
            this.getName(), previous, consumingStrategy, afterMatchSkipStrategy);
```

**顺序很重要**：`new Pattern(...)` 时就把 `previous` 和 `consumingStrategy` 传进去，建立链表关系。

```java
    // ③ 条件：where / or
    if (condition != null) {
        IterativeCondition iterativeCondition =
                condition.toIterativeCondition(classLoader);
        if (iterativeCondition instanceof RichOrCondition) {
            pattern.or(iterativeCondition);
        } else {
            pattern.where(iterativeCondition);
        }
    }
```

**为什么 OR 要特殊处理？** Flink 的 `pattern.or(cond)` 会把 cond 加到"或条件集合"；而 AND/NOT/AVIATOR/CLASS 都走 `where`。`RichOrCondition` 是 Flink 内部对 `or(...)` 的封装，所以这里用它判断。

```java
    // ④ 量词
    TimesSpec times = quantifier.getTimes();
    for (Quantifier.QuantifierProperty prop : quantifier.getProperties()) {
        switch (prop) {
            case OPTIONAL: pattern.optional(); break;
            case GREEDY:   pattern.greedy(); break;
            case LOOPING:
                if (times != null) {
                    pattern.timesOrMore(times.getFrom(), times.getWindowTime());
                }
                break;
            case TIMES:
                if (times != null) {
                    pattern.times(times.getFrom(), times.getTo());
                }
                break;
            default: break;
        }
    }
```

| 属性 | Flink API | 说明 |
|---|---|---|
| `OPTIONAL` | `optional()` | 可选节点 |
| `GREEDY` | `greedy()` | 贪婪 |
| `LOOPING` | `timesOrMore(from, windowTime)` | 至少 from 次，上不封顶 |
| `TIMES` | `times(from, to)` | 固定范围 |

注意 `SINGLE` 属性不在 switch 中处理——因为它对应"默认行为"，无需调用 API。

```java
    // ⑤ 内部连续性（仅对可重复节点）
    if (!quantifier.getProperties().contains(Quantifier.QuantifierProperty.SINGLE)) {
        Quantifier.ConsumingStrategy inner = quantifier.getConsumingStrategy();
        if (inner == Quantifier.ConsumingStrategy.SKIP_TILL_ANY) {
            pattern.allowCombinations();
        } else if (inner == Quantifier.ConsumingStrategy.STRICT) {
            pattern.consecutive();
        }
    }
```

**为什么限制在非 SINGLE？** `consecutive()` / `allowCombinations()` 只对**循环节点**（times/oneOrMore）有意义；单次节点调用会抛异常。

**注意：`SKIP_TILL_NEXT` 不需要调用**——它是循环内部的默认行为。

```java
    // ⑥ untilCondition
    if (quantifier.getUntilCondition() != null) {
        IterativeCondition untilCond =
                quantifier.getUntilCondition().toIterativeCondition(classLoader);
        pattern.until(untilCond);
    }

    return pattern;
}
```

`until` 只能在循环节点上使用。

### 5.3 `fromPattern()`

```java
public static NodeSpec fromPattern(Pattern<?, ?> pattern) {
    QuantifierSpec q = new QuantifierSpec(
            pattern.getQuantifier(), pattern.getTimes(), pattern.getUntilCondition());
    ConditionSpec c = ConditionSpec.fromCondition(pattern.getCondition());
    return new NodeSpec(pattern.getName(), q, c, PatternNodeType.ATOMIC);
}
```

---

## 6. `ConditionSpec` 体系精读

### 6.1 多态声明

```java
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
              include = JsonTypeInfo.As.EXISTING_PROPERTY,
              property = "type", visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = ClassConditionSpec.class,    name = "CLASS"),
    @JsonSubTypes.Type(value = AviatorConditionSpec.class,  name = "AVIATOR"),
    @JsonSubTypes.Type(value = RichOrConditionSpec.class,   name = "OR"),
    @JsonSubTypes.Type(value = RichAndConditionSpec.class,  name = "AND"),
    @JsonSubTypes.Type(value = RichNotConditionSpec.class,  name = "NOT"),
})
```

`EXISTING_PROPERTY`：`type` 字段在 JSON 里已存在，Jackson 用它判断子类型，同时保留给业务字段。

### 6.2 各子类还原逻辑

| 子类 | `toIterativeCondition` |
|---|---|
| `AviatorConditionSpec` | `new AviatorCondition<>(expression)`（第 04 篇） |
| `ClassConditionSpec` | `classLoader.loadClass(className).getDeclaredConstructor().newInstance()` |
| `RichOrConditionSpec` | 至少 2 个子条件，两两折叠成 `RichOrCondition` |
| `RichAndConditionSpec` | 至少 2 个子条件，两两折叠成 `RichAndCondition` |
| `RichNotConditionSpec` | 1 个子条件，包成 `RichNotCondition` |

**折叠构造示例（OR）：**

```java
IterativeCondition<?> result = new RichOrCondition(
        conditions.get(0).toIterativeCondition(classLoader),
        conditions.get(1).toIterativeCondition(classLoader));
for (int i = 2; i < conditions.size(); i++) {
    result = new RichOrCondition(result, conditions.get(i).toIterativeCondition(classLoader));
}
```

即 `OR(a,b,c,d)` → `OR(OR(OR(a,b),c),d)`。AND 同理。

### 6.3 `fromCondition()`：反向

```java
public static ConditionSpec fromCondition(IterativeCondition<?> condition) {
    if (condition == null) return null;
    if (condition instanceof RichOrCondition)  return new RichOrConditionSpec(...);   // 递归
    if (condition instanceof RichAndCondition) return new RichAndConditionSpec(...);  // 递归
    if (condition instanceof RichNotCondition) return new RichNotConditionSpec(...);  // 递归
    if (condition instanceof AviatorCondition) return new AviatorConditionSpec(...);
    return new ClassConditionSpec(condition.getClass().getName());
}
```

**兜底策略**：认不出的条件一律当 `CLASS` 存全限定类名——要求该类有 public 无参构造，才能被还原。

---

## 7. 其余 spec 类

### 7.1 `QuantifierSpec`

```java
public QuantifierSpec(Quantifier quantifier, Quantifier.Times times,
                      IterativeCondition<?> untilCondition) {
    this.consumingStrategy = quantifier.getConsumingStrategy();
    this.properties = new ArrayList<>();
    for (Quantifier.QuantifierProperty prop : Quantifier.QuantifierProperty.values()) {
        if (quantifier.hasProperty(prop)) this.properties.add(prop);
    }
    this.times = times != null
            ? new TimesSpec(times.getFrom(), times.getTo(), times.getWindowTime()) : null;
    this.untilCondition = untilCondition != null
            ? ConditionSpec.fromCondition(untilCondition) : null;
}
```

从 Flink `Quantifier` 枚举出所有已激活的属性（`SINGLE/LOOPING/TIMES/OPTIONAL/GREEDY`）。

### 7.2 `TimesSpec`

```java
private final int from;
private final int to;
private final Time windowTime;   // 循环内窗口，可空
```

对应 `times(from, to)` / `timesOrMore(from).within(windowTime)`。

### 7.3 `WindowSpec`

```java
private final String type;   // FIRST_AND_LAST / PREVIOUS_AND_CURRENT
private final int size;
private final String unit;

@JsonIgnore
public Time getTime() {
    return Time.of(size, TimeUnit.valueOf(unit));
}
```

- `getType()` 转 `WithinType`
- `getTime()` 是**计算属性**，用 `@JsonIgnore` 避免被序列化
- `fromWindowTime` 反向构造

### 7.4 `EdgeSpec`

```java
private final String source;
private final String target;
private final Quantifier.ConsumingStrategy type;
```

`type` 直接复用 Flink 的枚举：`STRICT / SKIP_TILL_NEXT / SKIP_TILL_ANY`。

### 7.5 `AfterMatchSkipStrategySpec`

```java
public AfterMatchSkipStrategy toAfterMatchSkipStrategy() {
    switch (type) {
        case "SKIP_TO_NEXT":        return AfterMatchSkipStrategy.skipToNext();
        case "SKIP_PAST_LAST_EVENT":return AfterMatchSkipStrategy.skipPastLastEvent();
        case "SKIP_TO_FIRST":       return AfterMatchSkipStrategy.skipToFirst(patternName);
        case "SKIP_TO_LAST":        return AfterMatchSkipStrategy.skipToLast(patternName);
        case "NO_SKIP":
        default:                    return AfterMatchSkipStrategy.noSkip();
    }
}
```

反向 `fromAfterMatchSkipStrategy` 通过**类名判断**（`contains("NoSkip")` 等），因为 Flink 的策略类没有公共类型标识。

---

## 8. 完整转换流程图

```
JSON
 │
 ├─ nodes[] ──► NodeSpec / GraphSpec（Jackson 多态）
 ├─ edges[] ──► EdgeSpec
 ├─ window  ──► WindowSpec
 └─ afterMatchStrategy ──► AfterMatchSkipStrategySpec
 │
 ▼ GraphSpec.toPattern()
 ├─ findBeginPatternName()          入度为 0 的起点
 ├─ 沿 edgeCache 串链
 │    └─ NodeSpec.toPattern()
 │         ├─ PatternWrapper（绕过 protected）
 │         ├─ condition.toIterativeCondition()
 │         ├─ quantifier：optional/greedy/times/timesOrMore
 │         ├─ consecutive()/allowCombinations()
 │         └─ until()
 └─ within(window)
 │
 ▼ Pattern（链尾节点，含完整 previous 链）
```

---

## 9. 设计要点与限制

| 要点 | 说明 |
|---|---|
| JSON 是规则定义 | 可序列化、可传输、无对象污染 |
| 图只支持线性链 | `edgeCache` 以 source 为 key，多分支会覆盖 |
| 单起点 | `findBeginPatternName` 要求入度 0 节点唯一 |
| COMPOSITE 嵌套 | `GraphSpec extends NodeSpec`，递归还原 |
| 条件多态 | 通过 `type` 字段分发 |
| `PatternWrapper` | 绕过 protected 构造器 |

**易错点：**

1. `edgeCache` 的"一个 source 只保留一条边"——不要在设计规则时画分支图。
2. 首节点的 `consumingStrategy` 无实际意义（固定 STRICT）。
3. `consecutive()/allowCombinations()` 只能用于循环节点。
4. `until` 只能用于循环节点。
5. `TimeDeserializer` 的 `unit` 必须大写且是 `TimeUnit` 枚举名。

---

## 10. 本篇小结

### 10.1 一句话总结

`GraphSpec` 把 Pattern 链建模成"节点 + 边"的有向图，`toPattern()` 以入度为 0 的节点为起点沿边串链，`NodeSpec.toPattern()` 负责单节点的条件/量词/连续性还原；JSON 与 Pattern 的双向转换让规则可以像数据一样存储和传输。

### 10.2 自测问题

1. `findBeginPatternName()` 的判定算法是什么？为什么要求唯一？
2. 为什么首节点的边类型固定用 `STRICT`？
3. `edgeCache` 为什么只支持线性链？
4. `consecutive()` / `allowCombinations()` 为什么只对非 SINGLE 节点调用？
5. `OR` 条件为什么要走 `pattern.or()` 而不是 `where()`？
6. `WindowSpec.getTime()` 为什么标 `@JsonIgnore`？

### 10.3 下一篇

→ `04-dynamic-condition.md`：`AviatorCondition` 如何把表达式字符串变成每条事件的布尔判定。
