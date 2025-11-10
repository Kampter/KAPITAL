#!/usr/bin/env bash
# optimize_aws_c7i.sh
# AWS c7i.large + Ubuntu 24.04 LTS 低延迟调优（ENA 友好，含 IRQ 绑核、A/B 开关）
set -euo pipefail

log(){ printf '[aws-opt] %s\n' "$*"; }

require_root(){
  if [[ ${EUID:-0} -ne 0 ]]; then
    log '请用 root 运行：sudo bash optimize_aws_c7i.sh'
    exit 1
  fi
}

# 可调环境变量（可在调用前 export）
LL_CPU="${LL_CPU:-1}"                 # 应用跑的 CPU（建议与 IRQ 分离：IRQ=0，应用=1）
BUSY_US="${BUSY_US:-0}"               # busy_poll/busy_read（默认 0；按需 A/B：50 或 100）
AWS_OFFLOAD_TUNE="${AWS_OFFLOAD_TUNE:-0}"  # 1=尝试关闭 GRO/GSO/TSO/调节合并；默认 0 不动
CHRONY_AWS="169.254.169.123"          # AWS 时钟源
CHRONY_POOL="${CHRONY_POOL:-time.cloudflare.com}"

detect_iface(){
  local iface
  iface=$(ip route get 1.1.1.1 2>/dev/null | awk '/dev/ {for(i=1;i<=NF;i++) if ($i=="dev"){print $(i+1); exit}}') || true
  [[ -z "$iface" ]] && iface=$(ip -o link show | awk -F': ' '/ens|eth|eno|enp/ {print $2; exit}') || true
  echo "$iface"
}

update_packages(){
  log '安装依赖：ethtool chrony numactl'
  apt-get update -y >/dev/null
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ethtool chrony numactl >/dev/null
}

configure_limits(){
  log '配置 limits（nofile / nproc）'
  cat >/etc/security/limits.d/99-aws-lowlatency.conf <<'LIM'
* soft nofile 1048576
* hard nofile 1048576
root soft nofile 1048576
root hard nofile 1048576
* soft nproc 262144
* hard nproc 262144
LIM
}

configure_sysctl(){
  log '配置 sysctl（网络/内存/可选 busy poll）'
  cat >/etc/sysctl.d/99-aws-lowlatency.conf <<SYS
# 队列与缓冲
net.core.somaxconn = 4096
net.core.netdev_max_backlog = 4096
net.core.rmem_default = 262144
net.core.rmem_max = 67108864
net.core.wmem_default = 262144
net.core.wmem_max = 67108864
net.ipv4.tcp_rmem = 4096 87380 67108864
net.ipv4.tcp_wmem = 4096 65536 67108864
net.ipv4.ip_local_port_range = 2000 65000

# WebSocket 长连接保持健康
net.ipv4.tcp_keepalive_time = 60
net.ipv4.tcp_keepalive_intvl = 10
net.ipv4.tcp_keepalive_probes = 6

# busy poll（默认 0；可通过 BUSY_US 环境变量开启 A/B）
net.core.busy_read = ${BUSY_US}
net.core.busy_poll = ${BUSY_US}

# 调度/内存
kernel.numa_balancing = 0
kernel.sched_rt_runtime_us = -1
vm.swappiness = 1
vm.dirty_ratio = 10
vm.dirty_background_ratio = 5
SYS
  sysctl -q -p /etc/sysctl.d/99-aws-lowlatency.conf
}

configure_chrony(){
  log '配置 chrony（AWS 时钟 + Cloudflare 兜底）'
  install -d -m 755 /etc/chrony/conf.d
  cat >/etc/chrony/conf.d/aws-lowlatency.conf <<CHR
server ${CHRONY_AWS} prefer iburst minpoll 4 maxpoll 4
server ${CHRONY_POOL} iburst minpoll 4 maxpoll 4
makestep 0.01 3
rtcsync
leapsecmode slew
CHR
  systemctl restart chrony || true
}

set_cpu_governor_best_effort(){
  # 许多云机不可调；能调则设为 performance，不报错
  log '尝试设置 CPU governor=performance（若不支持将忽略）'
  if systemctl list-units --full -all | grep -q '^ondemand'; then
    systemctl disable --now ondemand.service >/dev/null 2>&1 || true
  fi
  local changed=0
  for gov in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    if [[ -w "$gov" ]]; then
      echo performance >"$gov" || true
      changed=1
    fi
  done
  [[ $changed -eq 1 ]] && log 'CPU governor 已设为 performance' || log 'CPU governor 不可调，跳过'
}

disable_thp(){
  log '禁用透明大页 THP'
  cat >/etc/systemd/system/disable-thp.service <<'THP'
[Unit]
Description=Disable Transparent Huge Pages for latency-sensitive workloads
After=multi-user.target

[Service]
Type=oneshot
ExecStart=/bin/bash -c 'for n in /sys/kernel/mm/transparent_hugepage/enabled /sys/kernel/mm/transparent_hugepage/defrag; do [ -w "$n" ] && echo never > "$n"; done'
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
THP
  systemctl daemon-reload >/dev/null 2>&1 || true
  systemctl enable --now disable-thp.service >/dev/null 2>&1 || true
}

nic_tuning(){
  local iface="$1"
  [[ -z "$iface" ]] && { log '未发现网络接口，跳过 NIC 调优'; return; }
  log "NIC: $iface  （将进行能力检测）"
  ethtool -i "$iface" || true
  ethtool -k "$iface" || true

  # 默认不动 offload/coalescing（ENA 默认表现很好），仅当 AWS_OFFLOAD_TUNE=1 时尝试
  if [[ "$AWS_OFFLOAD_TUNE" == "1" ]]; then
    log 'AWS_OFFLOAD_TUNE=1 → 尝试关闭 GRO/GSO/TSO / 调整合并为低延迟（若不支持将忽略）'
    ethtool -K "$iface" gro off gso off tso off  >/dev/null 2>&1 || true
    ethtool -C "$iface" rx-usecs 0 rx-frames 1 tx-usecs 0 tx-frames 1 >/dev/null 2>&1 || true
    ethtool -G "$iface" rx 4096 tx 4096 >/dev/null 2>&1 || true
  else
    log '保持 ENA 默认 offload/合并参数（推荐起点）'
  fi

  # 关闭 RPS/RFS，避免软中断跨核
  if [[ -f "/sys/class/net/$iface/queues/rx-0/rps_cpus" ]]; then
    for q in /sys/class/net/"$iface"/queues/rx-*; do echo 0 > "$q/rps_cpus" || true; done
  fi
  if [[ -f "/sys/class/net/$iface/queues/rx-0/rps_flow_cnt" ]]; then
    for q in /sys/class/net/"$iface"/queues/rx-*; do echo 0 > "$q/rps_flow_cnt" || true; done
  fi
}

pin_irqs_with_systemd(){
  local iface="$1"
  log "绑定 $iface 的 IRQ 到 CPU0（应用建议跑在 CPU${LL_CPU}）"

  install -d -m 755 /opt/latency
  cat >/opt/latency/rebind_irqs.sh <<'REB'
#!/usr/bin/env bash
set -euo pipefail
IFACE="${IFACE:-$(ip route get 1.1.1.1 2>/dev/null | awk "/dev/ {for(i=1;i<=NF;i++) if (\$i==\"dev\"){print \$(i+1); exit}}")}"
CPU="${IRQ_CPU:-0}"   # 默认把 IRQ 绑到 CPU0
if [[ -z "$IFACE" ]]; then echo "No IFACE"; exit 0; fi
PATH_IRQ="/sys/class/net/${IFACE}/device/msi_irqs"
if [[ -d "$PATH_IRQ" ]]; then
  for i in $(ls "$PATH_IRQ"); do echo "$CPU" >"/proc/irq/$i/smp_affinity_list" || true; done
else
  for i in $(grep -E " ${IFACE}\b" /proc/interrupts | awk -F: '{print $1}'); do echo "$CPU" >"/proc/irq/$i/smp_affinity_list" || true; done
fi
REB
  chmod +x /opt/latency/rebind_irqs.sh

  cat >/etc/systemd/system/rebind-irqs.service <<SVC
[Unit]
Description=Rebind ENA IRQs to CPU0
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
Environment="IFACE=$iface"
Environment="IRQ_CPU=0"
ExecStart=/opt/latency/rebind_irqs.sh
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
SVC
  systemctl daemon-reload
  systemctl enable --now rebind-irqs.service >/dev/null 2>&1 || true

  log "运行策略建议：应用进程固定在 CPU${LL_CPU}，例如： taskset -c ${LL_CPU} uv run python okx_latency_bench.py"
}

print_next(){
  cat <<'EOS'

[aws-opt] ✅ 优化已应用。建议步骤：
1) 重启一次（可选），确保 THP/IRQ 绑核/limits/chrony 配置持续生效。
2) 校验：
   - chronyc tracking / sources -v  （看到 169.254.169.123 为 prefer 且偏差 < 500μs）
   - ethtool -i/-k IFACE（确认 ENA、能力集）
   - /proc/interrupts（确认网卡 IRQ 落在 CPU0）
3) 跑基准（单进程）：
   taskset -c ${LL_CPU} uv run python okx_latency_bench.py
4) 需要 A/B：
   - busy poll：BUSY_US=0 vs 50（导出变量后重跑脚本）
   - AWS_OFFLOAD_TUNE=1（仅当观察到 p99 尾部偶发拉长时测试）
5) 订阅增多时：拆进程，books5/trades 各自独立连接+独立 CPU。

Tip:
- 若要实时优先级（需 root）： sudo chrt -f 80 taskset -c ${LL_CPU} <your-cmd>
- 建议把生产策略与写盘/风控拆进程，避免单事件循环拥挤。
EOS
}

main(){
  require_root
  update_packages
  configure_limits
  configure_sysctl
  configure_chrony
  set_cpu_governor_best_effort
  disable_thp

  local iface; iface="$(detect_iface)"
  if [[ -z "$iface" ]]; then
    log '未发现网络接口，退出'; exit 1
  fi
  nic_tuning "$iface"
  pin_irqs_with_systemd "$iface"
  print_next
}

main "$@"
