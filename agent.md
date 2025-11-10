### 项目简介
1. 使用uv这个python管理工具
2. 使用 picows 这个低延迟的websokect library
3. 获取okx中hype_usdt的trader和books数据
4. 需要一个低延迟的延迟性能监控工具，不会影响数据获取和后续处理计算
5. 直接打印关键信息，不需要持久化存储

### v0.1需求
1. 使用 orjson 解析 picows 来的数据
2. 修改延迟统计到us级别
3. 使用 orjson + Numpy ring buffer
4. 自动计算每帧延迟（OKX ts → 本地 ts）
5. 输出实时 Δlatency 指标（p50/p95）
6. 修复trader数据
输出：main.py

### v0.2需求
1. okx_latency_bench.py是我在gcp东京的测试脚本，如有需要你可以调整这个代码
2. 我需要你写一个.sh脚本，自动化一键优化gcp上面的c4云主机
3. 我的云主机：2c4gb，ubuntu 24.04 lts
4. 最终目的: 连接到okx的websocket行情数据，极低的延迟，目前测试下来orjson解析的延迟只有3-6us，完全不是性能瓶颈，目前瓶颈来自websocket和linux内核优化