#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[okx-opt] %s\n' "$*"
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    log '请使用root权限运行：sudo ./optimize_gcp_c4.sh'
    exit 1
  fi
}

update_packages() {
  local pkgs=(ethtool cpufrequtils chrony numactl)
  log "安装依赖: ${pkgs[*]}"
  apt-get update >/dev/null
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${pkgs[@]}" >/dev/null
}

configure_limits() {
  log '配置文件描述符和进程限制'
  cat <<'LIM' >/etc/security/limits.d/99-okx-lowlatency.conf
* soft nofile 1048576
* hard nofile 1048576
root soft nofile 1048576
root hard nofile 1048576
* soft nproc 262144
* hard nproc 262144
LIM
}

configure_sysctl() {
  log '配置网络和内核低延迟参数'
  cat <<'SYS' >/etc/sysctl.d/99-okx-lowlatency.conf
# 网络队列
net.core.somaxconn = 4096
net.core.netdev_max_backlog = 4096
net.core.rmem_default = 262144
net.core.rmem_max = 67108864
net.core.wmem_default = 262144
net.core.wmem_max = 67108864
net.ipv4.tcp_rmem = 4096 87380 67108864
net.ipv4.tcp_wmem = 4096 65536 67108864
net.ipv4.tcp_fastopen = 3
net.ipv4.tcp_low_latency = 1
net.ipv4.tcp_mtu_probing = 1
net.ipv4.tcp_timestamps = 1
net.ipv4.tcp_sack = 1
net.ipv4.tcp_syncookies = 0
net.ipv4.tcp_tw_reuse = 1
net.ipv4.ip_local_port_range = 2000 65000
net.ipv4.tcp_fin_timeout = 10
net.ipv4.tcp_keepalive_time = 60
net.ipv4.tcp_keepalive_intvl = 10
net.ipv4.tcp_keepalive_probes = 6

# 调度器和内存
kernel.numa_balancing = 0
kernel.sched_rt_runtime_us = -1
vm.swappiness = 1
vm.dirty_ratio = 10
vm.dirty_background_ratio = 5
SYS
  sysctl -q -p /etc/sysctl.d/99-okx-lowlatency.conf
}

configure_chrony() {
  log '配置chrony低漂移NTP'
  install -d -m 755 /etc/chrony/conf.d
  cat <<'CHR' >/etc/chrony/conf.d/okx-lowlatency.conf
server metadata.google iburst maxpoll 4
server time.cloudflare.com iburst maxpoll 4
makestep 0.01 3
rtcsync
leapsecmode slew
CHR
  systemctl restart chrony || true
}

set_cpu_governor() {
  log '设置CPU性能模式'
  if systemctl list-units --full -all | grep -q '^ondemand'; then
    systemctl disable --now ondemand.service >/dev/null 2>&1 || true
  fi
  for gov in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    [[ -w "$gov" ]] && echo performance >"$gov"
  done
  cat <<'CPU' >/etc/systemd/system/set-performance-governor.service
[Unit]
Description=Set CPU governor to performance for low-latency workloads
After=network.target

[Service]
Type=oneshot
ExecStart=/bin/bash -c 'for gov in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do [ -w "$gov" ] && echo performance > "$gov"; done'
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
CPU
  systemctl daemon-reload >/dev/null 2>&1 || true
  systemctl enable --now set-performance-governor.service >/dev/null 2>&1 || true
}

disable_thp() {
  log '禁用透明大页'
  cat <<'THP' >/etc/systemd/system/disable-thp.service
[Unit]
Description=Disable Transparent Huge Pages for latency-sensitive workloads
After=multi-user.target

[Service]
Type=oneshot
ExecStart=/bin/bash -c 'for node in /sys/kernel/mm/transparent_hugepage/enabled /sys/kernel/mm/transparent_hugepage/defrag; do [ -w "$node" ] && echo never > "$node"; done'
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
THP
  systemctl daemon-reload >/dev/null 2>&1 || true
  systemctl enable --now disable-thp.service >/dev/null 2>&1 || true
}

nic_tuning() {
  local iface
  iface=$(ip route get 1.1.1.1 2>/dev/null | awk '/dev/ {for(i=1;i<=NF;i++) if ($i=="dev") {print $(i+1); exit}}')
  if [[ -z "$iface" ]]; then
    iface=$(ip -o link show | awk -F': ' '/ens|eth|eno|enp/ {print $2; exit}')
  fi
  if [[ -z "$iface" ]]; then
    log '未找到网络接口，跳过NIC调优'
    return
  fi
  log "调优网络接口: $iface"
  if command -v ethtool >/dev/null 2>&1; then
    ethtool -K "$iface" gro off gso off tso off rxhash off >/dev/null 2>&1 || true
    ethtool -K "$iface" tx-nocache-copy on >/dev/null 2>&1 || true
    ethtool -C "$iface" rx-usecs 0 rx-frames 1 tx-usecs 0 tx-frames 1 >/dev/null 2>&1 || true
    ethtool -G "$iface" rx 4096 tx 4096 >/dev/null 2>&1 || true
  fi
}

irqbalance_disable() {
  log '关闭irqbalance以避免CPU迁移'
  systemctl disable --now irqbalance >/dev/null 2>&1 || true
}

main() {
  require_root
  update_packages
  configure_limits
  configure_sysctl
  configure_chrony
  set_cpu_governor
  disable_thp
  nic_tuning
  irqbalance_disable
  log '优化完成，建议重启一次生效全部设置（或按需重启相关服务）'
}

main "$@"
