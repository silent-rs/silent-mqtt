# Silent MQTT 🛰️

**Silent MQTT** 是 [Silent Odyssey](https://github.com/silent-rs/silent-odyssey) 路线图中的第一阶段实验项目，目标是在 Silent 框架上实现一个**最小可用的 MQTT Broker**，并通过互通与压测验证其生产级潜力。

---

## 🎯 项目目标
- 实现 **MQTT 协议最小子集**（连接、订阅、发布、QoS 0/1/2）。
- 提供 **互通性验证**（与 `mosquitto_pub/sub` 等客户端通信）。
- 在 **高并发场景下压测**，验证连接保持与消息投递性能。
- 为后续协议实验提供可复用的基础模块。

---

## 🧪 功能范围
- ✅ CONNECT / CONNACK
- ✅ SUBSCRIBE / SUBACK
- ✅ PUBLISH / PUBACK / PUBREC / PUBREL / PUBCOMP（QoS 0/1/2）
- ✅ DISCONNECT
- ✅ 保活心跳（Keep Alive）
- ✅ 遗嘱消息（Last Will）

---

## ⚡ 压测方法
推荐使用 [mqtt-bench](https://github.com/ralphlange/mqtt-bench) 或 [mosquitto-clients](https://mosquitto.org/)。

示例（1000 并发连接，持续 30 秒）：
```bash
mqtt-bench -broker tcp://127.0.0.1:1883 -clients 1000 -count 10000
```

指标收集：
- 吞吐量（msg/s）
- 平均/最大延迟
- 内存/CPU 占用
- 连接稳定性（掉线率）

---

## 🔗 互通测试
1.	启动 Silent MQTT broker：
```bash
cargo run
```

2.	使用 Mosquitto 测试：
```bash
mosquitto_sub -h 127.0.0.1 -t test
mosquitto_pub -h 127.0.0.1 -t test -m "hello from Silent"
```

3.	验证输出：订阅端应收到消息。

---

## 📂 目录结构

```bash
silent-mqtt/
├── src/
│   ├── main.rs          # 入口，启动 broker
│   ├── broker/          # broker 核心逻辑
│   └── protocol/        # MQTT 协议解析
├── benches/             # 压测脚本与工具
├── docs/                # 互通记录与性能报告
└── README.md
```

---

## ✅ 完成标准 (Definition of Done)
- 	实现并验证最小可用的 MQTT Broker。
- 	在 mosquitto_pub/sub 下成功互通。
- 	给出至少一份压测结果报告（QPS、延迟、资源占用）。
- 	文档化部署与测试方法。
