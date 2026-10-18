# remote-skill 设计说明

## 目标

让「在远程机器上干活」这件事对 Agent 和脚本来说和本地一样顺：

- 状态持久（工作目录、环境变量、shell 配置、后台任务）
- 连接断开不丢工作
- 明确的退出码与结构化输出
- 不引入新的信任面（仍走 SSH，不开放网络端口）

## 进程模型

```
本地 rsk ──ssh(stdin/stdout)──► rskilld bridge ──unix socket──► rskilld serve
                                                                （守护进程/路由）
                                                                        │
                                                     ~/.rsk/run/sessions/<name>.sock
                                                      ┌─────────────────┴─────────────────┐
                                              keeper main                        keeper build
                                        (bash -l -i + pty + socket)        (bash -l -i + pty + socket)
```

* `bridge` 是每次调用都会执行的薄壳：连着 socket 就把 stdio 转发过去；socket 不
  可用就 `setsid` 拉起 `serve` 再连。`serve` 与 ssh 会话完全解耦。
* **keeper** 是每会话一个的独立进程：`rskilld session --name X --socket ...`。
  它持有 pty、shell、滚动缓冲和会话完成信号，是会话真正“活着”的地方。
  守护进程只维护到各 keeper 的连接、把会话级请求路由过去、并转发事件帧。
* 守护进程在 `~/.rsk/run/rskd.sock`（0600）上监听，socket 文件名带 pid 文件
  `rskd.sock.pid` 便于清理；启动时若发现陈旧 socket 会先删除。
* keeper 的 socket 与元数据在 `~/.rsk/run/sessions/<name>.sock|.json`，
  元数据含 pid、版本、api、binary sha，用于接管与清理。
* 守护进程日志：`~/.rsk/log/rskd.log`（超过 8MB 轮转一次）。

### 为什么把会话挪出守护进程

守护进程是升级与重连的“易变层”：客户端升级会重启它、用户会 `rsk down` 它、
进程可能崩溃。只要会话还活在守护进程里，这些操作都会杀死正在跑的命令。
把会话放进 keeper 之后：

| 事件 | 守护进程内会话（旧） | keeper（现在） |
|---|---|---|
| `rsk down` | 会话结束 | 会话继续 |
| 客户端/守护进程升级重启 | 会话结束 | 会话继续 |
| 守护进程被 kill -9 | 会话结束 | 命令继续跑，输出可从新守护进程的 `logs` 取回 |
| 机器重启 | 会话结束 | 会话结束（快照恢复环境变量） |

### 路由与接管

* 会话级方法（`exec`、`session.attach|resize|signal|scrollback|close`）到了守护
  进程后，按参数里的 `session` 名找到 keeper，分配一个新的请求 id 转发过去，
  并把返回的 `res`/`ev` 帧改写回客户端 id —— 纯帧级代理，方法实现只有一份。
* `session.open` 由守护进程处理：确保 keeper 存在（接管或新建）后返回会话信息。
* 守护进程启动时异步 `scan()` 会话目录；socket 存在且能握手就接管，握手失败且
  元数据里的 pid 已死就清理残留文件。
* keeper 用协议 API 版本握手；版本不兼容的 keeper 不会被接管，会在下次使用时
  重建（同 API 的旧构建继续运行，这正是会话能跨升级存活的原因）。

### 关闭顺序（踩过的坑）

`ShutdownAll` 里先停 keeper、再关连接、最后删 socket，整个过程可能持续几秒。
`Serve()` 在监听器关闭后立刻返回，若进程此时退出，尚未完成的 keeper 停止流程
会被截断，表现为“`down --kill-sessions` 有时没杀掉会话”。因此 `ShutdownDone`
通道在关闭流程真正结束后才关闭，`serve()`/keeper 都等待它再返回。
客户端在确认会话 socket 消失后才打印结果。

## 会话

启动方式：`/bin/sh -c 'stty raw -echo; exec bash -l -i'`，通过 pty 运行。

* `-l -i` 保证 `/etc/profile`、`~/.bash_profile`、`~/.bashrc` 按用户预期加载
  （nvm/conda/pyenv 之类的初始化因此可用）
* `stty raw -echo`：关闭回显（否则守护进程写进去的命令会被读回来污染输出）、
  关闭输出后处理（不再 `\n`→`\r\n`）、关闭 ISIG（信号由守护进程显式投递）
* `PS1/PS2/PS3/PS4=""`、关闭 bracketed paste：终端输出里没有提示符和转义噪声
* 初始环境补充 `TERM=xterm-256color`、`PAGER=cat`、`GIT_PAGER=cat`、
  `GIT_TERMINAL_PROMPT=0`，避免分页器和凭据提示把 Agent 卡死（可用 `--env` 覆盖）

### 命令执行与完成检测

会话启动时注入两个函数：

```bash
__rsk_emit() { printf '%s\t%d\t%s\n' "$1" "$2" "$PWD" >&3; }
__rsk_run()  { ... cd "$3" ...; eval "$2"; __rsk_emit "$1" "$?"; export -p > 状态快照; }
```

每条命令写成一行 `__rsk_run '<id>' '<命令>' '<cwd>'`：

* `eval` 在主 shell 里执行 → `cd`/`export`/函数定义持久生效
* 完成信号走 **fd 3**（独立管道，不是终端流），因此不需要在输出里找标记，
  二进制输出、超长输出都不会误判
* 状态行同时回传 `$PWD`，守护进程据此跟踪会话工作目录
* 命令执行完把 `export -p` 快照写 `~/.rsk/state/<session>.env`，会话被重建时
  自动 source 回来（近似恢复环境）

### 中断与超时

终端是 raw 模式，`\x03` 不会产生信号，所以：

1. 守护进程用 `tcgetpgrp(pty master)` 找到**前台作业的进程组**，只对作业组发
   SIGINT，shell 本身不受影响；
2. 若作业已消失但状态行还没回来（交互式 bash 在前台作业被 SIGINT 杀死后
   会丢弃当前输入行剩余部分，这是实测行为），守护进程轮询前台进程组，发现
   控制权回到 shell 后**合成**完成状态（rc=130）并补一个换行，保证会话可继续；
3. 作业顽固时升级 SIGTERM → SIGKILL；若仍占据终端，返回
   `unfinished: true` 而不是伪造 rc=0；
4. shell 处在提示符（含续行）状态时，才写 `\x03` 让 readline 放弃当前输入行。

超时（`--timeout`）和客户端中断（Ctrl-C）走同一路径；`--timeout 0` 关闭。

### 并发

每个会话有一个令牌，同一时刻只允许一条命令，避免多条命令在同一个 shell 里交错。
`--wait` 控制等待时长（默认 30s），超时返回 `busy`。要真并行就开多个会话
（`--session build`），守护进程支持任意数量会话。

## 文件传输

两个方向都用 tar 流，因此权限、软链、空目录、mtime 一次性解决：

* 上传：客户端边打包边发送 `ev{in}` 分片；守护进程 `io.Pipe` → 增量解包，
  天然背压。写临时文件后 rename，不会出现半截可执行文件。
* 下载：守护进程打包 → `ev{out}` 分片 → 客户端解包。
* 安全：解包端拒绝 `..` 逃逸、绝对符号链接指向外部；Windows 上创建软链失败时
  降级为文本文件并在 warning 中说明。
* 命名规则（两个方向一致）：目标是已存在目录或带结尾分隔符 → 放到该目录下并用
  源的名字；目标不存在 → 该路径就是副本本体。

## 端口转发

`rsk forward` 在本地监听，每个连接发一条 `tunnel.open` 请求；守护进程用
`net.Dial` 从服务器侧拨号，随后双向 `ev{out}`/`ev{in}` 复用同一 ssh 连接。
支持 `port`、`lport:rport`、`lport:rhost:rport`、`bind:lport:rhost:rport`。

反向转发（`--remote`，ssh -R 语义）由 `tunnel.listen` 承载：守护进程在服务器上
监听，每接受一条连接就发一个带 `conn` 标识的 `open` 事件，随后在同一请求 id 上
用 `out`/`in`/`eof`/`close` 帧多路复用；客户端按 `conn` 标识在本地拨号并把字节
泵回隧道。监听器在守护进程内，因此守护进程重启后反向转发需要重建（与 ssh -R 一致）。

## 会话状态持久化（跨机器重启）

keeper 会周期性（以及每条命令完成后、按 2s 节流）把会话状态写入
`~/.rsk/state/<name>.json`：工作目录、最近 100 条命令、最多 256KB 滚动缓冲；
环境变量仍走 `<name>.env`。新 keeper 启动时读取这些文件：cwd 作为初始目录、
滚动缓冲写入 ring（带一条 “session restored” 标记）、命令列表写回 shell 历史
文件，于是 `rsk logs`、`pwd`、`history` 都能看到重启前的内容。机器重启会丢进程，
但不再丢上下文。

## Windows 远端

Windows 没有可用的 ConPTY（creack/pty 在 Windows 是空实现），因此 Windows 会话
是**虚拟会话**：keeper 不持有 shell 进程，而是为每条命令启动一个 PowerShell
子进程，脚本开头注入 `Set-Location <上次 cwd>` 与上次的环境变量，结尾把新的
`Get-Location` 和环境快照写回状态文件，所以 `cd`、`$env:X` 跨命令保留，且
stdout/stderr 天然分离、没有 pty 回显。代价：没有交互式 attach（`rsk sh` 返回
明确错误）、每条命令一次 PowerShell 启动开销（`--no-profile` 可显著加速）。
超时/中断用 `taskkill /T /F` 结束整棵子进程树而保留会话。守护进程、keeper 与
AF_UNIX socket 在 Windows 上同样可用；二进制经 scp 上传，并在旁边生成
`bridge.cmd` 启动脚本以避免 shell 引号问题。

## 协议

```
帧:  [4 字节大端长度][JSON 信封]
信封: {t: hello|welcome|req|res|ev|bye, id, m, p, r, e, c, d, x}
事件通道: out(输出) in(输入/上传) eof done error signal meta
方法: info ping session.open/list/close/attach/resize/signal/scrollback
      exec file.put file.get file.list tunnel.open daemon.shutdown
```

握手校验 API 版本；守护进程欢迎包里带版本、平台、用户、home、socket、会话列表
以及**自身二进制 sha256**——客户端据此判断远端跑的是不是同一份构建，不一致就
自动重启守护进程（`--restart`）。

大输出不丢包：订阅者队列满时先阻塞（背压），30s 仍无进展才断开该订阅者，
会话本身继续跑，结果留在滚动缓冲里。

## 失败模式与恢复

| 情况 | 行为 |
|---|---|
| 远端子进程不在/被删 | `bridge` 启动失败 → 客户端重新引导上传后重试一次 |
| 守护进程被杀 | 下次调用自动拉起；**会话与正在跑的命令继续**，输出留在滚动缓冲 |
| ssh 断开 / 客户端 Ctrl-C | 守护进程与命令继续运行；重新连接后 `rsk logs` 可见 |
| 命令 `exit` 掉 shell | 会话标记 closed；下次 exec 自动重建并 cd 回原目录 |
| 远端构建版本不一致 | 客户端打印一行提示并自动重启守护进程 |
| ssh 主机密钥未确认 | 明确提示先手工 `ssh <host>` 一次 |
| 显式停止会话 | `session.close` → 关闭 shell 并删除 `~/.rsk/state/<name>.env` 快照 |
| keeper 进程被杀 | 下次使用同名会话时重建；环境变量从快照恢复 |

## 未做与路线图

- 远端为 Windows 的支持（守护进程代码已按平台分层，缺 conpty 会话与路径适配）
- 反向转发 `--remote`（远端口 → 本地服务）目前相当于 `ssh -R`，未实现
- 会话跨机器重启（当前只持久化环境快照与工作目录，不恢复进程与滚动缓冲）
- 多跳/跳板机的显式建模（现在依赖 ssh 自己的 ProxyJump 配置）
- 客户端连接复用（Windows OpenSSH 不支持 ControlMaster；Linux/macOS 已启用）
