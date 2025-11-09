### 项目简介
1. 使用uv这个python管理工具
2. 使用 picows 这个低延迟的websokect library
3. 获取okx中hype_usdt的trader和books数据
4. 需要一个低延迟的延迟性能监控工具，不会影响数据获取和后续处理计算
5. 直接打印关键信息，不需要持久化存储

### v0.1新需求
1. 使用 orjson 解析 picows 来的数据
2. 修改延迟统计到us级别
3. 使用 orjson + Numpy ring buffer
4. 自动计算每帧延迟（OKX ts → 本地 ts）
5. 输出实时 Δlatency 指标（p50/p95）
6. 修复trader数据