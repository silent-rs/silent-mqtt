# 项目需求整理

## 背景
构建基于 Silent 框架的最小可运行 MQTT Broker，以验证 Silent 在生产环境的潜力。

## Silent Odyssey 对齐目标
- 作为 Silent Odyssey 第一阶段产出，需要提供一个可验证的 MQTT Broker，以证明 Silent 协议层的可扩展性。
- Broker 必须支持基础互通（以 mosquitto 客户端为基准），并输出最小可行示例。
- 需要沉淀协议适配经验，形成可复用的 `Protocol` 实现范式，便于后续在 Odyssey 中扩展更多网络协议。
- 在 Odyssey 规划中，本项目需为性能验证打基础，至少提供压测基线（连接数、吞吐、延迟）供后续阶段对比。

## 技术栈与基础要求
- 编程语言：Rust。
- 运行时：Tokio 异步运行时。
- ID 生成：`scru128` 库，禁止使用 `uuid`。
- 时间处理：默认使用 `chrono::Local::now().naive_local()` 表示当前真实时间。
- 框架：优先使用 Silent 生态组件。
- 包管理：Rust 端使用 `cargo`，前端暂不涉及。

## 协议支持范围（MQTT 3.1.1 核心子集）
1. CONNECT / CONNACK
2. SUBSCRIBE / SUBACK
3. PUBLISH / PUBACK（支持 QoS 0/1/2）
4. PUBREC / PUBREL / PUBCOMP（QoS 2 交互完整流程）
5. PINGREQ / PINGRESP（保活检测，超时断开并触发遗嘱）
6. DISCONNECT
7. 遗嘱消息（Last Will Message）

以下功能暂不实现：持久会话等高级特性。

## 架构规划
```
src/
├── main.rs          # 入口，启动 TCP Listener
├── protocol/        # 协议解析、编码与 Silent Protocol 适配
├── broker.rs        # Broker 核心逻辑（订阅表、消息路由）
└── client.rs        # 客户端会话管理
```

## 迭代步骤
1. **协议编解码基础**：实现 `protocol` 模块中 CONNECT/CONNACK 报文解析与编码。
2. **基础握手流程**：在 `main.rs` 中监听 1883 端口，接入客户端，完成 CONNECT → CONNACK。
3. **订阅与转发**：实现 SUBSCRIBE/SUBACK、PUBLISH/PUBACK，管理订阅表并转发消息。
4. **连接保活**：支持 PINGREQ/PINGRESP 与 DISCONNECT，保证会话生命周期正确。
5. **使用示例**：更新 `README.md`，提供启动与使用 `mosquitto` 客户端的验证流程。

## 质量要求
- 每个迭代阶段保持代码可编译、可运行。
- 遵循 Rust 格式化与默认 lint 规则。
- 变更后需执行 `cargo check` 或 `cargo clippy` 验证，无需额外测试脚本。
- 文档除 `README.md` 外归档于 `docs/` 目录。
