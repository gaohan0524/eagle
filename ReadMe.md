# eagle

Real time data processing system based on Flink and CEP (Drools / Siddhi)

中文详情：[基于flink和drools的实时日志处理](https://www.cnblogs.com/luxiaoxun/p/13197981.html)

### Design:
![design](https://github.com/luxiaoxun/eagle/blob/master/picture/eagle-design.png)

### Modules:
* eagle-api：基于 Spring Boot，提供策略规则的读写服务。
* eagle-common：通用类模块。
* eagle-alert：基于 Flink 和 [Siddhi](https://github.com/siddhi-io/siddhi) 的数据告警服务。
* eagle-log：基于 Flink 和 [Drools](https://github.com/kiegroup/drools) 的日志处理服务。
* eagle-detection：基于 Flink CEP 的交易欺诈检测服务（Dynamic CEP），支持运行时规则热更新。

### Tech Stack
* Java 8
* Flink 1.17.2
* Spring Boot 2.5.12
* Kafka / MySQL / Elasticsearch / Redis

### Documents
* [eagle-detection 代码精读系列](docs/eagle-detection/README.md)：12 篇，按学习顺序拆解 Dynamic CEP 源码。
* [eagle-detection 与 Flink CEP 学习指南](docs/eagle-detection-flink-cep-study.md)：Flink CEP 基础原理 + 模块整体讲解。
* [Eagle Dynamic CEP 设计与使用](docs/eagle-dynamic-cep.md)：规则 JSON 字段、运行配置、REST API、FAQ。

### Build
```bash
mvn clean package
```

### IDE dev mode:
```
"Run/Debug" => "Edit Configurations...",
"Program Arguments" => "--mode dev"
"Build and run" section select "Modify options" => Java => Add dependencies with "Provided" scope to classpath.
```

### Cluster mode:
```bash
# eagle-log
flink run -m yarn-cluster -ynm eagle-log -j eagle-log.jar -c com.alarm.eagle.EagleLogApp -arg "--mode test"

# eagle-detection
flink run -m yarn-cluster -ynm eagle-detection -j eagle-detection.jar -c com.alarm.eagle.DetectionApp -arg "--mode prod"
```

### eagle-api:
```
http://localhost:8080/eagle-api/swagger-ui/index.html
```
