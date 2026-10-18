# remote-skill (rsk)

给 Agent 和脚本用的「远程工作区」工具：像 VS Code Remote-SSH 那样，通过 SSH
把一台远程服务器变成你的工作目录——上传客户端、常驻守护进程、持久终端会话，
然后在本地下命令、传文件、转发端口。

```
本地                          服务器 (linux / macOS)
┌──────────────┐   ssh -T    ┌────────────────────────────────────────┐
│ rsk (CLI)    │ ──────────► │ ~/.rsk/bin/rskilld                     │
│  · 主机配置  │  stdio 隧道 │   └─ __bridge ── unix socket           │
│  · 内嵌二进制│ ◄────────── │        └─ rskilld serve (守护进程/路由) │
└──────────────┘             │             └─ run/sessions/*.sock     │
                             │                  ├─ keeper main         │
                             │                  │   └─ bash -l -i (pty) │
                             │                  └─ keeper build        │
                             │                      └─ bash -l -i (pty) │
                             └────────────────────────────────────────┘
```

每个会话是一个**独立进程**（keeper），持有自己的 pty、shell 与 socket。守护进程只做
注册表和协议路由，因此它被杀、升级、`rsk down` 都不影响正在跑的会话与命令（tmux 语义）。

## 它解决什么问题

普通 `ssh host "命令"` 每次都是全新的 shell：`cd`、`export`、后台任务、screen
状态全部丢失，长任务还会被连接中断带走。`rsk` 在服务器上常驻守护进程与若干
**独立于守护进程的持久 pty 会话**，因此：

- `cd` / `export` / shell 函数 / 别名跨命令保留（等价于你真的坐在那台机器前）
- 断线、守护进程重启、升级、乃至 `kill -9` 守护进程，命令都继续跑；结果与输出
  留在滚动缓冲里，随时 `rsk logs` 取回
- 每次调用只付一次轻量 SSH 握手，而不是重新登录、重新配环境
- 传文件、端口转发、后台任务、超时中断都是一等公民
- 双向端口转发：`--remote` 让服务器监听并把连接送回本机（`ssh -R`）
- 机器重启后仍可恢复上下文：工作目录、已导出环境变量、滚动缓冲、命令历史落盘
- 远端除 Linux/macOS/FreeBSD 外支持 **Windows**（虚拟会话，见下）
- 下载流式解包（大文件不占内存）；端口转发会自动重连
- 为 Agent 设计：`--json` 结构化输出、`--timeout` 兜底、`--detach` 跑服务、
  退出码原样透传

## 快速开始

```powershell
# Windows: 构建（会自动交叉编译并内嵌各平台远程端二进制）
pwsh build.ps1

# 安装 rsk 到 PATH，并可顺带安装 Agent Skill
pwsh install.ps1 -InstallSkill
```

```bash
# Linux / macOS
make            # 或 ./build.sh
./install.sh --install-skill
```

连接一台机器（第一次会自动探测平台、上传 `rskilld`、启动守护进程）：

```bash
rsk up devbox --workspace ~/project           # 直接用 ~/.ssh/config 里的别名
rsk up 10.0.0.5 --user ubuntu -i ~/.ssh/id_ed25519 --name gpu -p 2222
rsk doctor devbox                             # 出问题时逐段诊断
```

之后用短名字操作：

```bash
rsk exec devbox -- npm test
rsk exec devbox -- "cd services/api && go build ./..."
rsk exec devbox --detach -- "npm run dev -- --port 3000"
rsk forward devbox 3000:127.0.0.1:3000
rsk put devbox ./src /home/me/project/src --recursive
rsk get devbox /home/me/project/dist/app ./dist/app
rsk logs devbox --tail 20000
rsk status devbox
rsk down devbox
```

## 命令速查

| 命令 | 说明 |
|---|---|
| `rsk up <host>` | 引导主机、启动守护进程、开会话（`--workspace` 设工作目录） |
| `rsk exec <host> [--] <cmd>` | 在持久会话里执行命令，退出码透传 |
| `rsk sh <host>` | 把会话接到本终端（`Ctrl-]` 断开） |
| `rsk put/get` | 上传/下载文件或目录（tar 流，保留权限/软链/空目录） |
| `rsk sync <host> <local> <remote>` | 增量同步（按大小+mtime 比对，`--exclude/--delete/--dry-run`） |
| `rsk ls <host> [path]` | 列目录 |
| `rsk ports <host>` | 列出服务器监听端口，并给出可复制的转发命令 |
| `rsk ps <host>` / `rsk kill <host> <job-id>` | 列出 / 停止 `--detach` 后台作业 |
| `rsk logs <host> [--follow]` | 查看/跟随会话终端输出 |
| `rsk status <host> [--json]` | 守护进程与会话状态 |
| `rsk forward <host> <spec>` | 本地端口转发到服务器侧目标 |
| `rsk forward <host> <spec> --detach` | 后台转发；断线或守护进程重启后自动重连 |
| `rsk forward <host> <spec> --remote` | 反向转发：服务器监听，连回本机服务（`ssh -R`） |
| `rsk forward <host> --socks [port]` | 本地 SOCKS5 代理，流量从服务器侧拨号（`ssh -D`） |
| `rsk down <host>` | 停止守护进程，**会话继续运行** |
| `rsk down <host> --kill-sessions` | 连会话一起停止（`--session NAME` 只停一个） |
| `rsk down <host> --purge` | 停止守护进程与会话，并清理远端状态 |
| `rsk hosts` / `rsk doctor` / `rsk version` | 主机列表 / 诊断 / 版本 |

常用开关：`--session`、`--timeout`、`--wait`、`--cwd`、`--env`、`--json`、
`--detach`、`--inline`、`--user`、`--port`、`--identity`、`--ssh-arg`、`--verbose`。
`--broker` 会复用一个本地常驻 SSH 连接：实测命令从 ~1.9s 降到 ~0.13s。
目前为实验特性（守护进程/会话反复重启时仍有边缘竞态），默认关闭；
需要时用 `rsk exec <host> --broker -- <cmd>` 或设置 `RSK_BROKER=1`。
其中会话级开关（`--session`/`--env`/`--shell`/`--inline`）只作用于本次调用，
只有 `rsk up` 会把它们写进主机配置作为默认值。
完整说明见 [skills/remote-workspace/reference/cli.md](skills/remote-workspace/reference/cli.md)。

## 接入 Agent Skill

`skills/remote-workspace/` 是一份可直接安装的 Codex Skill：它告诉模型什么时候用
`rsk`、怎么避免把会话堵死、超时/后台/并发的正确姿势，以及常见故障的处理。

```powershell
pwsh install.ps1 -InstallSkill          # 复制到 $env:CODEX_HOME\skills\remote-workspace
```

```bash
./install.sh --install-skill            # 复制到 ${CODEX_HOME:-~/.codex}/skills/remote-workspace
```

## 工作原理

1. **引导**：`rsk` 用系统 `ssh` 探测远端（`uname -sm`、`$HOME`、`id -un`、
   `sha256sum`），从内嵌资源里取出对应平台的 `rskilld`，经 stdin 流式上传到
   `~/.rsk/bin/<version>/`，校验 sha256 后落盘。
2. **常驻**：`ssh host ~/.rsk/bin/.../rskilld bridge` 把 ssh 的 stdio 接到
   `~/.rsk/run/rskd.sock`；socket 不存在时，`bridge` 以 `setsid` 方式拉起
   `rskilld serve`（脱离 ssh 会话，stdout 落日志），因此断线不影响它。
3. **会话独立**：每个会话由单独的 `rskilld session` 进程（keeper）承载，
   socket 在 `~/.rsk/run/sessions/<name>.sock`，元数据在同名 `.json`。
   守护进程启动时扫描该目录并**接管**已有的 keeper；keeper 只在会话结束、
   `down --kill-sessions`/`rsk down --session` 或超时回收时才退出。
4. **会话**：keeper 启动 `bash -l -i`（pty + `stty raw -echo`），命令通过 shell
   函数在**当前 shell** 里 `eval`，所以状态天然持久；完成信号用独立 fd 3 管道
   回传，不解析输出，不会与命令输出混淆。环境快照写在
   `~/.rsk/state/<name>.env`，会话被彻底停止时会一并删除。
5. **协议**：4 字节长度前缀 + JSON 信封（请求/响应/事件流）。守护进程把会话级
   请求按 session 名路由到对应 keeper，并双向转发帧，客户端无需知道分层。
6. **升级**：守护进程上报自身二进制的 sha256；客户端发现与内嵌版本不一致时
   自动重启守护进程——**会话不受影响**。keeper 之间用协议版本号把关，版本不
   兼容时才会重建。

更多细节：[docs/design.md](docs/design.md)。

## 安全说明

**仅有密码登录的服务器**（无 sudo、不能写 authorized_keys）也能用：

```bash
rsk login devbox            # 交互输入一次密码（DPAPI 加密保存 + 固定服务器主机指纹）
rsk up devbox               # 之后所有命令都走 plink，无需再输入
rsk logout devbox           # 删除已保存的密码
```

实现上使用 PuTTY 的 `plink`/`pscp`（需安装 PuTTY），密码以 Windows DPAPI 用户密钥
加密存放于 `~/.rsk/credentials/`，不写入配置文件；无凭据时 ssh 不会偷偷等待输入，
而是快速失败并提示先 `rsk login`（需要人工输入时加 `--ask-pass`）。
注意：plink 的 `-pw` 参数会出现在本机进程命令行里（同一用户可见）；每条命令仍需
一次 plink 登录（约 2s），本地连接复用尚未实现。

- 传输完全走你已有的 SSH（密钥、ssh-agent、ProxyJump、`~/.ssh/config`、2FA 都继续生效）
- 远端守护进程**只监听 unix socket**（`~/.rsk/run/rskd.sock`，权限 0600，目录 0700），
  不开放任何网络端口；同一台机器上的其他用户无法访问
- 本机配置 `~/.rsk/config.json` 记录主机、端口、密钥路径，权限 0600
- 远程执行以登录用户身份进行，权限等价于你自己 `ssh` 上去执行
- Windows 客户端受 OpenSSH 限制不支持连接复用，每次调用会新建连接（功能不受影响）

## 限制与取舍

- Windows 远端是「虚拟会话」：无 pty（`rsk sh` 不可用，`exec`/`logs`/传文件/转发可用），
  每条命令启动一个 PowerShell 进程（约 200ms，`--no-profile` 可加速），
  `cd` 与 `$env:X` 通过快照跨命令保留
- 会话是 pty 语义：stdout/stderr 合并，可能出现颜色转义（与真实终端一致）
- 一个会话同时只跑一条命令；并行请用 `--session`
- 会话独立于守护进程，但仍在同一台机器上：机器重启会丢会话（`rsk exec` 会自动
  重建，并从快照恢复已导出环境变量）
- `rsk up --inline` 可退回旧行为（会话在守护进程内，随其退出而结束）
- 会话由同名 socket 标识，名字相同的会话不能在同一台机器上并存

## 开发与测试

```bash
go test ./...                                   # 单元测试
go run ./tools/pack                             # 交叉编译远程端并内嵌
go build -o dist/rsk ./cmd/rsk

# 端到端：对真实 ssh 服务器跑 20 项行为验证
pwsh test/e2e.ps1 -Target <ip> -User <user> -Key <path-to-key>
```

E2E 覆盖：引导上传、会话持久（cwd/env）、退出码（含 `exit` 掉 shell 后自愈）、
超时中断与会话自愈、语法错误、多会话、递归上传下载（两种命名规则）、单文件改名、
ls、后台任务日志、滚动日志、相对路径工作区、端口转发，以及**守护进程重启 / `down`
不断会话 / 守护进程被 `kill -9` 时正在跑的命令仍在运行 / `--kill-sessions` 真正停止会话**。

## 目录结构

```
cmd/rsk/          本地 CLI（命令解析、输出格式）
cmd/rskilld/      远端守护进程 + stdio 桥 + selftest
internal/proto/   帧协议与信封
internal/daemon/  会话（pty、fd3 完成信号、中断）、服务端分发、文件、隧道
                  会话 keeper（独立进程）、守护进程代理路由、状态快照
internal/client/  ssh 传输、引导上传、RPC 客户端、交互 attach、端口转发
internal/transfer/ tar 打包与安全解包
internal/assets/  内嵌的 rskilld 二进制
tools/pack/       交叉编译并内嵌
skills/remote-workspace/  Agent Skill
test/e2e.ps1      端到端测试
```
