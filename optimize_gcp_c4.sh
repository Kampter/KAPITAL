#!/usr/bin/env bash
# optimize_gcp_c4.sh
# GCP c2/c4 (Ubuntu 24.04) 低延迟调优（gVNIC 友好，含 IRQ 绑核、能力检测）
set -euo pipefail

log() { printf '[okx-opt] %s\n' "$*"; }

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    log '请使用 root 权限运行：sudo bash optimize_gcp_c4.sh'
    exit 1
  fi
}

# 可调环境变量
LL_CPU="${LL_CPU:-0}"            # 低延迟进程与 NIC IRQ 绑定到的 CPU（建议选 0 或 1）
BUSY_US="${BUSY_US:-50}"         # busy_poll/busy_read 微秒
CHRONY_POOL="${CHRONY_POOL:-time.cloudflare.com}"
NTP_GCP="metadata.google"

detect_iface() {
  local iface
  iface=$(ip route get 1.1.1.1 2>/dev/null | awk '/dev/ {for(i=1;i<=NF;i++) if ($i=="dev") {print $(i+1); exit}}')
  if [[ -z "$iface" ]]; then
    iface=$(ip -o link show | awk -F': ' '/ens|eth|eno|enp/ {print $2; exit}')
  fi
  echo "$iface"
}

update_packages() {
  local pkgs=(ethtool cpufrequtils chrony numactl hping3)
  log "安装依赖: ${pkgs[*]}"
  apt-get update -y >/dev/null
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${pkgs[@]}" >/dev/null
}

configure_limits() {
  log '配置 limits（nofile / nproc）'
  cat >/etc/security/limits.d/99-okx-lowlatency.conf <<'LIM'
* soft nofile 1048576
* hard nofile 1048576
root soft nofile 1048576
root hard nofile 1048576
* soft nproc 262144
* hard nproc 262144
LIM
}

configure_sysctl() {
  log '配置 sysctl（网络/调度/内存）'
  cat >/etc/sysctl.d/99-okx-lowlatency.conf <<SYS
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

# WebSocket 长连接下列项影响有限，但保持稳健
net.ipv4.tcp_timestamps = 1
net.ipv4.tcp_sack = 1
net.ipv4.tcp_mtu_probing = 1
net.ipv4.tcp_fin_timeout = 10
net.ipv4.tcp_keepalive_time = 60
net.ipv4.tcp_keepalive_intvl = 10
net.ipv4.tcp_keepalive_probes = 6

# busy poll（按需 A/B）
net.core.busy_read = ${BUSY_US}
net.core.busy_poll = ${BUSY_US}

# 调度/内存
kernel.numa_balancing = 0
kernel.sched_rt_runtime_us = -1
vm.swappiness = 1
vm.dirty_ratio = 10
vm.dirty_background_ratio = 5
SYS
  sysctl -q -p /etc/sysctl.d/99-okx-lowlatency.conf
}

configure_chrony() {
  log '配置 chrony（低漂移）'
  install -d -m 755 /etc/chrony/conf.d
  cat >/etc/chrony/conf.d/okx-lowlatency.conf <<CHR
server ${NTP_GCP} iburst maxpoll 4
server ${CHRONY_POOL} iburst maxpoll 4
makestep 0.01 3
rtcsync
leapsecmode slew
CHR
  systemctl restart chrony || true
}

set_cpu_governor() {
  log '设置 CPU performance governor'
  if systemctl list-units --full -all | grep -q '^ondemand'; then
    systemctl disable --now ondemand.service >/dev/null 2>&1 || true
  fi
  for gov in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    [[ -w "$gov" ]] && echo performance >"$gov"
  done

  cat >/etc/systemd/system/set-performance-governor.service <<'CPU'
[Unit]
Description=Set CPU governor to performance for low-latency workloads
After=network.target

[Service]
Type=oneshot
ExecStart=/bin/bash -c 'for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do [ -w "$g" ] && echo performance > "$g"; done'
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
CPU
  systemctl daemon-reload >/dev/null 2>&1 || true
  systemctl enable --now set-performance-governor.service >/dev/null 2>&1 || true
}

disable_thp() {
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

nic_tuning() {
  local iface="$1"
  if [[ -z "$iface" ]]; then
    log '未找到网络接口，跳过 NIC 调优'
    return
  fi
  log "NIC 调优目标接口: $iface"

  if ! command -v ethtool >/dev/null 2>&1; then
    log 'ethtool 不存在，跳过能力检测'
    return
  fi

  # 显示能力（便于事后核对）
  ethtool -i "$iface" || true
  ethtool -k "$iface" || true

  # 在 gVNIC (gve) 上，部分 offload/coal 参数可能不支持；均做 best-effort
  ethtool -K "$iface" gro off gso off tso off >/dev/null 2>&1 || true
  ethtool -K "$iface" tx-nocache-copy on >/dev/null 2>&1 || true
  ethtool -C "$iface" rx-usecs 0 rx-frames 1 tx-usecs 0 tx-frames 1 >/dev/null 2>&1 || true
  ethtool -G "$iface" rx 4096 tx 4096 >/dev/null 2>&1 || true

  # 关闭 RPS/RFS（避免软中断跨核；按需再打开）
  if [[ -f "/sys/class/net/$iface/queues/rx-0/rps_cpus" ]]; then
    for q in /sys/class/net/"$iface"/queues/rx-*; do echo 0 > "$q/rps_cpus" || true; done
  fi
  if [[ -f "/sys/class/net/$iface/queues/rx-0/rps_flow_cnt" ]]; then
    for q in /sys/class/net/"$iface"/queues/rx-*; do echo 0 > "$q/rps_flow_cnt" || true; done
  fi
}

pin_irqs_and_doc() {
  local iface="$1"
  log "绑定 $iface 的 MSI-X IRQ 到 CPU${LL_CPU}"
  local irqs_path="/sys/class/net/${iface}/device/msi_irqs"
  if [[ -d "$irqs_path" ]]; then
    for irq in $(ls "$irqs_path"); do
      echo "${LL_CPU}" >"/proc/irq/${irq}/smp_affinity_list" || true
    done
  else
    # 兼容路径：根据名称搜索（可能是 gve- ）
    for irq in $(grep -E " $iface|gve" /proc/interrupts | awk -F: '{print $1}'); do
      echo "${LL_CPU}" >"/proc/irq/${irq}/smp_affinity_list" || true
    done
  fi

  # 生成一个 helper，开机后可一键重绑
  install -d -m 755 /opt/okx
  cat >/opt/okx/rebind_irqs.sh <<REB
#!/usr/bin/env bash
set -euo pipefail
IFACE="${iface}"
CPU="${LL_CPU}"
IRQS="/sys/class/net/\${IFACE}/device/msi_irqs"
if [[ -d "\$IRQS" ]]; then
  for i in \$(ls "\$IRQS"); do echo "\$CPU" >"/proc/irq/\$i/smp_affinity_list" || true; done
else
  for i in \$(grep -E " \$IFACE|gve" /proc/interrupts | awk -F: '{print \$1}'); do echo "\$CPU" >"/proc/irq/\$i/smp_affinity_list" || true; done
fi
REB
  chmod +x /opt/okx/rebind_irqs.sh

  # systemd 服务，确保重启后自动生效
  cat >/etc/systemd/system/okx-rebind-irqs.service <<SVC
[Unit]
Description=Rebind NIC IRQs to CPU${LL_CPU}
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/opt/okx/rebind_irqs.sh
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
SVC
  systemctl daemon-reload
  systemctl enable --now okx-rebind-irqs.service >/dev/null 2>&1 || true
}

print_next_steps() {
  cat <<'EOS'

[okx-opt] ✅ 所有设置已应用。建议：
1) 重启一次，确保 IRQ 绑核 / governor / THP / chrony 全面生效。
2) 运行：chronyc tracking / sources -v 观察时钟偏移 < 500 μs。
3) 用：ethtool -k/-c/-g <iface> 核对能力（确认无“伪生效”）。
4) 跑你的基准：taskset -c ${LL_CPU} python3 okx_latency_bench.py

Tip:
- 需要 A/B busy_poll：导出环境 BUSY_US=0 再运行脚本、对比延迟分布。
EOS
}

main() {
  require_root
  update_packages
  configure_limits
  configure_sysctl
  configure_chrony
  set_cpu_governor
  disable_thp

  local iface
  iface="$(detect_iface)"
  if [[ -z "$iface" ]]; then
    log "未发现可用网络接口，退出"
    exit 1
  fi
  nic_tuning "$iface"
  pin_irqs_and_doc "$iface"
  print_next_steps
}

main "$@"
