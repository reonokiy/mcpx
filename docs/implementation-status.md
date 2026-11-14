# mcpx 实现进度

持续目标仍为 [完整技术方案](technical-design.md) 中的全部功能。下表是当前证据与缺口的索引，不替代技术方案中的逐项验收；尚未完成整体计划，也不缩小目标范围。

## 已有实现

十个 crate 的依赖方向已经建立；mcpx-wasi 提供 WASIp2 HTTP 驱动，mcpx-config 共享配置模型/解析。工具描述和入参 Schema 从远端发现，库通过运行时绑定、Serialize 参数构造器、JSON、字符串和 argv 调用；不生成工具专属 Rust SDK。HostRuntime 与 Machine 属于无 I/O 核心，原生 HTTP/stdio、浏览器 Wasm 和 WASI Component 复用协议与校验逻辑。

Rust async Client 提供凭据解析、授权身份隔离、目录 TTL 和完整刷新、绑定失效、checked-call、显式 continuation、取消与事件。核心和凭据 future 保留非 Send 支持。原生 QueuedRemote 在专用线程持有这些状态，通过 Send/Sync 句柄与调用方通信。

## 当前边界与验收缺口

| 范围 | 当前证据 | 仍需完成 |
| --- | --- | --- |
| 远程绑定与四种输入 | Core/Native/QueuedBinding 的 args_from/call_typed/submit_typed、结构化 deserialize 与运行示例；动态 Schema 校验 | 公开 API 稳定性、其余平台完整示例与更多输入/结果互操作 |
| CLI | 命名 HTTP/stdio 配置、直接 URL、servers/tools/call/complete、args-file、三种输出、Ctrl-C；Bash/Zsh/Fish 的动态工具/参数/枚举补全 | 更多 shell 版本/平台/词中光标、名称冲突/文件输入/非 UTF-8、终端输出边界测试 |
| Schema | Draft 2020-12、精确 vocabulary 策略、SchemaRegistry、可配置 128 项校验器 LRU、原生分配测量、引用/条件/数值校验 | 完整编译/缓存内存硬预算、全 Schema CPU/工作预算、更多 Schema 互操作和配置边界 |
| 协议 | 两种显式版本与 Auto、分页、JSON/SSE、路由/参数 header；共享通知资源、原生 HTTP/stdio 队列及 JS/Worker 自动驱动、WASI 协作式驱动；Core/Wasm/WIT 的旧版通知 GET 游标恢复，原生/JS/Worker/WASI 通知高层显式控制和 opt-in 有界恢复；Core/Wasm/WIT 业务 SSE 显式恢复、Native/JS/Worker/WASI 高层有界策略；原生初始连接、mutable 与 Queue 及 JS/Worker 的 opt-in session 续建 | 更多 SDK/宿主包装、WASI 自动 session 生命周期、完整版本/能力/错误矩阵 |
| 核心并发 | 最多 16 个在途调用，独立解码和一次性结果，乱序/单流故障/取消测试 | 更完整的竞争、跨 owner 误用/释放与资源预算矩阵 |
| 原生 HTTP 并发 | typed/JSON、字符串/argv、恢复共用 16 路；独立通知资源、显式启动/停止/状态/游标恢复控制和 opt-in 有界 GET 自动恢复；业务响应流有界 GET 策略，初始发现及已连接 mutable/队列的 opt-in 有界 session 续建；真实 16 路屏障、凭据/目录等待和官方 SDK 验证 | 更多准入/关闭/通知竞争、平台与完整事件矩阵、连接级状态 API |
| 原生 stdio | 共享子进程 16 路、分帧/写入/取消、官方 SDK；目录订阅及显式控制、显式重连与 opt-in 有界自动恢复、跨 epoch ID/绑定与不重放验证 | 更广平台、恢复/关闭/凭据竞争和连接级状态 API |
| 结果与交互 | 输出状态、保留原文的 call_checked、受约束 continuation；Rust/CLI/Wasm/WIT/JS/Worker 的执行证据、重试建议与可选操作 ID | 更广终态/取消/清理竞争、完整错误矩阵与公开接口稳定性 |
| 事件 | Core/原生/Worker 协议预算与丢失计数；OperationEvents、原生有界生命周期订阅、JS/Worker onOperationEvent 覆盖排队与控制操作，终态预留/单次结算 | 更广关闭/释放/消费者竞争、更多宿主异步消费包装与发布接口稳定性 |
| 目录与凭据 | HostRuntime 共享 TTL/身份策略；Rust、JS/Wasm/jco、Worker、WASI 队列的异步 provider、刷新、身份变化失效、原凭据清理旧会话；原生/JS/WASI 排队显式刷新合并；有界目录观察 | 更广竞争与 API 稳定性、其余订阅/宿主控制、持久化/资源缓存及预算 |
| 凭据来源 | env 默认解析、自定义命名引用、非秘密身份 ID、debug 脱敏 | 系统凭据存储、OAuth 登录、更多租户/提供者集成 |
| Web | 跨域 Chromium、自动探测、SSE、恢复；JS/Wasm/jco 的 16 路队列、Worker、补全与 CLI 示例；保留 settled Promise 的真实 GC 验证 | 更多凭据/取消竞争、更广浏览器/打包验证、完整释放竞争与稳定宿主包 |
| WASI | jco typed Component/16 路调用；独立 CLI/驱动、HTTP 配置；WasiQueue 的 16 路 mixed/恢复、异步目录/provider、Future 句柄、本地 runner、FIFO 控制/刷新合并；WasiReactor 的依赖式提交/外部 poll、通知游标恢复与有界策略；同步 Client/Queue/Reactor 的显式 HTTP 重连；Wasmtime 对接 rmcp 3.10；配置 CA 的 HTTPS embedding host/SDK 网关成功路径及信任拒绝 | 更多 runtime/executor 服务集成、完整 ABI/预算/生命周期、更广 SDK/运行时、原版 CLI 公共根 HTTPS 及平台矩阵 |
| 交付与质量 | 锁文件、本机检查、Rust 示例、三平台 Wasm 测试、CI 配置；1.88/1.100 最低编译器与实际运行验证 | 远端 CI 实际运行、性能报告、发布接口、文档与实现最终一致性 |
| 后续扩展 | 继续保留完整技术方案中的范围 | 持久化缓存、OAuth、更多 MCP 能力及其余完整范围继续按原计划推进 |

## 原生并行调度

默认最多 16 个 typed/JSON、字符串/argv 与 continuation 恢复调用同时在途；QueueOptions.max_in_flight 可设为 1..16。数量预算默认为 64（含当前准入和在途操作），输入 JSON 与记录计费预算为 8 MiB。队列 deadline 默认 60 秒，包括排队时间，并收紧到原生 driver 的 deadline。数量和字节预算独立，超限立即返回 QueueFull。参数构造器在本地完成 Schema 检查，完成后的参数可跨线程传递。

HTTP worker 仅拥有 reqwest、请求和冻结的凭据；Machine、绑定、provider 和回调仍在所有者线程。响应通过 16 条消息的有界通道拉取，字节块最多 64 KiB。凭据 future 只借用 driver，其等待期间继续处理其他调用的响应、取消、deadline、事件和完成，不重新启动 provider。

每次准入保留凭据快照，身份变化阻止新旧绑定混用。单请求错误不会清除其他请求的状态；现代 HTTP 取消关闭响应流，旧版 HTTP 另发 request-specific 取消通知。完成、取消和关闭都会终止并等待相应 reader。整个队列关闭时不再发送等待中的调用。

原生 HTTP/stdio 并发事件保留 core operation_id，同时携带 queue_operation_id。它们已通过 17 次提交的结果/事件关联测试；队列生命周期现有独立订阅，元数据控制请求在创建时关联队列 ID；初次连接发生于原生队列创建之前，仍由协议事件表示。

命令与 continuation 恢复已经进入同一并行调度，按提交顺序准入、允许乱序完成。绑定、refresh、配置、事件排空和 continuation 释放仍是屏障。目录 TTL/目录变更或身份变更需要等待已有调用结束后再刷新；如果变化出现在 provider 等待期间，保留本次凭据并继续处理其他 I/O，不重新解析 provider。紧邻准入时跨 TTL 边界也使用同一上下文等待后重试准入，不重试已发业务请求。原生 driver deadline 收紧到此等待阶段，发送前再次检查取消、接收方关闭和期限。

Core 的 call_tool_with 提供单次预检的串行命名参数构造，admit_prepared_tool 提供外部预检后的新绑定准入；ControlFlow::Break 是 help 等本地结果。admit_prepared_resume 复用现有句柄的身份/期限/Schema/单次消费约束。Command 扩展只负责参数解析；普通 Rust 字符串/argv 调用也不再拆成 fresh_tool 和 call 两次预检。

HTTP 测试要求 mixed typed/字符串/argv 16 个请求到齐才响应，第 17 个排队，事件关联全部提交 ID。另有 16 路表单恢复、输入更正、取消一个恢复后独立命令仍完成、重复恢复不发包；help/参数错误可在其他调用期间本地返回。目录变更在 provider 等待时到达，测试确认旧 I/O 正常完成、一次刷新后按新整数 Schema 解析命令且 provider 只调用一次；身份变化后新命令重绑，旧绑定/continuation 不越过身份，旧请求保留原 bearer。等待独占时取消未派发恢复，其句柄仍可用。命令与恢复各有 driver deadline 终态与复用验证。

## 共享 stdio 通道

子进程由持续 reader、writer 和 stderr 排空任务管理；每条读入帧最多 8 MiB，通过 16 条消息的通道交给所有者。Reader 持有半条 JSON/UTF-8 帧，单调用取消和控制操作切换仅转移已解码消息的 receiver，不销毁分帧状态。有序 writer 保证同一帧不会被取消消息或其他请求插入。

已发送调用取消后，迟到响应按已分配的过去 ID 忽略；未分配 ID、非法现代 server request、坏帧或 EOF 会结束共享通道。阻塞写的预算由 frame 持有到丢弃/完成，不因调用 future 提前结束而释放。关闭先停止 stdin，reader 在宽限期丢弃并排空输出，再回收子进程并等待三个 I/O 任务结束。

自有 fixture 验证现代、旧版、两种 Auto 模式下 16 个调用先到达同一 PID 才返回；另验证半个 UTF-8 字符时取消后仍可处理刷新和新调用、坏帧结束 16 个调用且终态不重复，以及阻塞写保留字节预算。新增 mixed typed/字符串/argv 屏障覆盖四种模式；现代与现代 Auto 另要求 16 个表单恢复先到齐，取消一个后其余返回且同一 PID 继续工作。官方 SDK 子进程在三种模式验证 mixed 16 路、现代 16 路恢复和正常 EOF 退出。

共享流没有关联信息的日志使用 operation_id 0，不伪装为最后一个调用的日志。直接 mutable Remote 的串行 stdio 中断仍关闭传输，现可显式 reconnect 后取得新绑定；独立复用取消和 opt-in 自动恢复属于 QueuedRemote 的所有者调度路径。

## stdio 目录订阅

QueueOptions.stdio_notifications 默认关闭，独立于 HTTP 开关。现代协议通过现有有序 writer 发送 subscriptions/listen，不占 16 个业务槽位；旧版只被动监听。共享 reader 在空闲、16 路业务在途和异步 provider 等待期间继续处理消息。ACK、目录变化与终态按订阅 RPC ID 精确关联；换订阅后的迟到帧不能使新目录失效。已观察的变化或订阅缺口使目录失效，后续操作按原有 fresh-read 策略刷新。

ACK 期限在 owner/provider 等待和串行元数据读取路径均受检查，只可收紧。缺 ACK、坏 ACK、拒绝和非法订阅消息只终止通知资源；非法共享 envelope/传输损坏仍终止共享通道。关闭/身份变化清理未完成订阅，干净终态及远端拒绝不额外取消。失败或完成后不自动重开；已接受的新身份或重新连接可开新订阅，自动进程恢复仍须显式 opt-in，且不重放业务。通知 writer 的失败通过独立状态唤醒 owner，不向 ProcessDriver 的共享读取通道注入异类控制消息。

Credential::scoped 提供无 bearer 的非秘密身份标签（非空、最多 256 UTF-8 字节），供宿主已在 HTTP 外认证时隔离绑定。标签本身不认证；stdio 原有拒绝 HTTP bearer 的限制不变。Core 新增 start_stdio_notifications 和可选的绝对单调毫秒 ACK deadline，宿主通过 stream_input 驱动并负责真实写入/取消；HTTP/SSE 接口和 WIT 布局未改。

新增 Core/真实子进程专项覆盖独立订阅与全部业务槽位、身份替换、迟到/坏帧隔离、缺 ACK、provider 等待、超时取消和 opt-in 进程恢复。四种 fixture 协议模式验证并发与按新整数 Schema 解析第 17 个调用；官方 rmcp 3.10.0 子进程在现代、旧版和 Auto 模式验证真实 ACK/通知、watcher 失效和正常关闭。完整构建与实际运行证据记录于文末。

## 原生通知控制

QueuedRemote 新增 start_notifications、stop_notifications，notification_status 转入独立控制通道；三者均有单次 with_timeout 变体。启动是普通 FIFO 屏障，解析一次当前凭据并按 TTL/身份刷新，已活动的监听器复用，结束或失败的监听器显式重开，返回时不要求 ACK 已到。停止会禁用自动监听，清理并 join 通知 I/O，使更早未完成的启动返回 Cancelled；较新的启动仍可执行。没有监听器时停止/查询返回 None，禁用状态跨 reconnect 保留。

停止和查询共享独立的两条记录、512 字节计费预算，不占普通业务容量，不解析 provider；16 路业务满载或调用/启动 provider 等待期间仍可执行。启用生命周期订阅时仍受其终态预留/ConsumerLagged 限制。串行元数据 I/O 和重连期间在安全 owner 边界执行控制，排队取消和 deadline 继续推进；停止启动操作不会截断正在写读的 stdio 元数据帧。尚未把这部分扩大为所有同步工作都可抢占的声明。

排队停止取消/到期没有监听副作用；开始清理后的取消/到期仍 join 清理，再返回相应错误，不恢复已停监听。控制保留独立队列 ID、一次终态和通常的 NotSent 业务执行证据。停止在 owner 执行后才撤销旧启动，取消的停止不会偷偷撤销它。新 Core Client::with_prepared_catalog 供调度器消费已解析的同一 DriverContext：要求空闲、应用授权/TTL/刷新，所有返回路径 finish，不再调用 provider。其未来被丢弃后的 abort 仍由宿主负责。

真实 HTTP 四种协议模式验证满载、独立停止/查询、重连后仍禁用；stdio 四种模式验证同一 PID 停止/重启及订阅/取消次数。另验证旧启动被及时撤销、更新的启动可执行、provider 等待只解析一次、两个预留名额/超时/取消/生命周期，以及 stdio 元数据阻塞时查询到期、停止在边界生效且 PID 继续工作。官方 rmcp 子进程在现代/旧版/Auto 各执行两轮显式启动、ACK/变化/停止与正常 EOF。测试临时目录改用时间和进程内序号，修复并行测试中的同名冲突。

## 旧版通知 GET 的显式游标恢复

ResponseDecoder 可选记录有界 SSE 元数据，普通请求解码默认不保留游标。完整事件（包括空 priming）提交 id，半帧不提交；无 id 事件继承前值，空 id 重置，NUL id 忽略，超过 4096 UTF-8 字节的 id 清除可恢复游标，直到新的可用 id。retry 只接受 ASCII 数字，零有效；无值默认 1000 ms。过大的重试间隔不向下截断；Core 拒绝超出宿主单调时钟范围的恢复。Debug 不打印不透明游标。

Core HostRuntime、browser Wasm 和 typed WIT 新增 notification_resume/resume_notifications。前者提供剩余等待时间，后者在期限到达后生成带 Last-Event-ID 的 GET，分配新 resource ID，不发送/重放工具 POST，不解析 provider，不清除断流导致的目录失效。宿主必须保留原 credential/session、排空 Core 和宿主已接受的能力回复，并关闭旧 reader；旧 resource 的迟到输入被拒绝。初次/恢复流断线或恢复 GET 开启时的传输失败可保留上一个已提交游标；显式 stop、身份/transport 变化、401/403、session 失效或完整坏消息不能恢复。半帧 EOF 保留原有 ProtocolError 诊断，但在最后完整游标后可以显式恢复。现代 HTTP/stdio 不使用这条路径。

共享 Node Wasm/jco/Chromium fixture 在两种显式版本和 Auto 共 12 个真实 HTTP 场景验证三个 GET、UTF-8 原始游标、服务端 retry 间隔、能力回复先完成、半帧/迟到输入隔离、空 id 重置，以及只有一次独立 tools/call POST。新 standalone WASI 示例在 Wasmtime 的四个显式/Auto 旧版/现代拒绝场景使用真实 wasi:http I/O，按原凭据回复并清理旧版 session；现代不产生 GET。固定 jco adapter 已更新，WIT 增加 typed resume record/method，必须重建组件/适配器。Unicode Last-Event-ID 在 Fetch Headers 使用 UTF-8 byte string，在 Rust/WASI 用原 UTF-8 bytes，未套用 MCP 参数 header 的 base64 编码。

原生 QueuedRemote 与高层 JS/Worker 的恢复接入见下节；WasiClientExt、队列与 Reactor 的恢复接入见“WASI 通知游标恢复”；原生业务响应流策略见“原生业务响应恢复”，其余高层接入和 404 会话续建等仍按完整方案推进。CI wasm job 通过 test:wasm 收录低层场景，msrv/wasi-cli 收录 Wasmtime 场景；是否实际远端执行仍以单独证据为准。

## 业务响应流的显式恢复基础

HostRuntime/Wasm/WIT 提供 enable_response_resumption、interrupt_response_for、response_resume、resume_response、response_replies、release_response_resumption 和 exhaust_response_resumption，固定 Component adapter 同步更新。只允许已派发的旧版 HTTP tools/call；原始 POST 响应开头之前 opt-in，普通 finish/fail 仍保持终态，不会复活一个已经失败/取消的调用。断线时显式 interrupt 丢弃最后完整 SSE 游标后的半帧，保留同一个 Pending call；GET 计划只换 action ID，保留 operation/RPC ID、进度 token、参数/Schema 快照、queue link 和预留终态。Core 不再生成新的工具 POST/MCP request，也不延长宿主总期限或替宿主选择重试策略。

GET 的 session/version 在原 POST 建立时冻结，另一个响应更新当前 session 不会改变它。Core 不持有 bearer；宿主必须保留原 client/credential/cancellation 快照，并 join 旧 reader 和能力回复 writer。跟踪路径支持旧版未实现 server request 的 -32601 回复；最多 16 条，ID 数值/文本有独立边界。回复未排空返回 Busy，游标只放 GET。原操作完成后还能排空同 chunk 接受的回复，随后释放 tracker；显式 release 只允许终态，取消/fail 直接清理。最多 16 个活跃/终态待释放 tracker，输入 chunk <=64 KiB，游标 <=4096 UTF-8 bytes。

早于 server retry 的恢复不分配新句柄。新句柄使用既有 Action ID 域，RPC/operation 域不被消耗，耗尽时不改坏旧 Pending；迟到旧 begin/push/消息被拒绝。现代 HTTP、stdio、元数据、未派发操作、JSON、无/重置/不安全游标、完整坏消息和回复队列溢出不走恢复。开启中的 GET 网络失败可保留原 checkpoint；401/403、带原 session 的 404、其他拒绝和非 SSE GET 终止原操作，执行证据仍 Unknown，不做会话续建。原调用最终完成只接受一次终态，继续使用原 output validation/context。

新增 13 项 Core 集成测试和一项 Machine ID 边界测试，覆盖 serial/concurrent/Auto、16 路全挂起保持业务槽、乱序恢复、能力回复与终态排空、取消、默认 EOF、游标/坏帧/请求 ID、重试溢出、身份/reset 边界、GET 状态/类型、tracker/chunk 上限、原 session 快照及最终 ID 原子性。真实 HTTP 在 Node Wasm/jco/跨域 Chromium 的两版本与显式/Auto 共 12 场景、Wasmtime 四 guest 验证三个响应流、UTF-8 Last-Event-ID、原 session/credential、reply drain、原样 uint64 结果与实际单个工具 POST。Native、JS/Worker 和 WASI 高层接入见对应小节；会话续建及其余完整范围继续推进。

官方 rmcp 3.10.0 的 Node Wasm/jco 显式旧版验证在途请求的 GET 重接：代理只截断前端首个完整 SDK priming，继续消费原后端流，不改事件/RPC 字节；SDK 的 session retry 配置为 10ms，测试工具保留 500ms 在途窗口，结果实际通过 resumed GET 到达。默认 LocalSessionManager 完成请求后可删除 request-wise 路由，不能据此宣称所有已完成响应均可重放；此前尝试等待三秒后恢复已完成流得到空 GET 的失败保留在日志。Auto 接到 SDK 明确的 UnsupportedProtocolVersion 时验证不降级、不 initialize/GET/call；不为测试绕过现有探测规则。需要长期完成结果重放的服务端可另提供其协议允许的 event store，但本批没有实现或宣称此服务端能力。

## 原生业务响应恢复

Client.with_response_recovery(ResponseRecoveryPolicy) 与 QueueOptions.response_recovery 默认关闭，只适用于已派发的旧版 HTTP tools/call。Some 队列配置覆盖 factory Client 策略，None 继承 builder；验证 1..8 个 GET、backoff <=60 秒、窗口 (0,600 秒]，无效队列配置在 factory/连接前拒绝。普通 mutable Remote 和并发队列复用同一个 ResponseIo 状态机，typed/JSON、字符串/argv 路径均进入既有调度；现代 HTTP、stdio、元数据和未启用场景保持原行为。通知与业务恢复分别配置。

从 reader 实际观察首个断线开始计算固定窗口，收紧到原调用 deadline；owner 等待、能力回复写入、server retry/backoff、GET 开启及读取消耗同一窗口，后续短 EOF 不重置次数或时间。旧 reader 与已接受 writer join 后才能 GET；恢复保持原 reqwest client、bearer/session/版本和原调用 RPC/operation/progress/Schema，不重跑 provider、初始化或业务 POST。保留 16 路业务槽及输入预算到最终排空，回复至多 16 个待写加一个 writer。队列在另一个调用的异步 provider 等待期间继续喂读流、推进回复与恢复时钟。

无可用游标、JSON、坏帧、认证/session/端点拒绝和 writer 失败终止恢复；尝试耗尽为 ResponseRecoveryExhausted，最终结果前执行证据维持 Unknown。取消/到期关闭并 join 当前 I/O，旧版取消通知继续使用原 POST 的 RPC 和凭据，不带 Last-Event-ID。mutable call future 被 drop 时 guard 立即 abort 现有 reader/writer，后续 cancel_active 清理 Core 并 join，再复用连接。原结果已被 Core 接受后，后续取消/到期不改写成功；回复清理共用 500ms 上限，owner close 直接 abort/join 余下 I/O 并保留已知结果。遗留取消通知另受原有 200ms 清理上限约束，不声明在截止时刻同步回收远端副作用。

新增 16 项真实 HTTP 集成测试，覆盖显式/Auto、冻结 Unicode 游标/凭据/session、能力回复前后排空、16 路恢复及 credits、pending provider、次数/总窗口、GET 开启/读取超时、默认关闭/现代/JSON/坏帧/无游标、拒绝、取消、mutable future drop、typed/字符串继承配置、成功与清理竞争、保留结果的 provider 回收。另增官方 rmcp 3.10 测试，普通客户端和并发队列均从 resumed GET 收到原 RPC 的真实最终结果，只有一个 tools/call POST 和一个 initialize。SDK 配置 10ms session retry 与 500ms 在途工具窗口；代理截断前端完整 priming 并继续排空原后端流，不改事件/RPC，不能据此推断 SDK 完成后永久保留结果。

## 原生通知游标恢复与自动策略

QueuedRemote.resume_notifications(resource_id) 及其 with_timeout 变体消费一个确切的旧版 HTTP 通知资源，不在等待期间改用最新身份/资源。与 stop/status 共用两条预留记录、512 字节计费预算，另有一个 resume permit，防止两次等待占完停止/查询空间。重试时间与已接受的能力回复写入都在 owner 调度中推进，不占 16 个业务槽位、不解析 provider。保留原 HTTP client/credential/session；同身份 token 轮换后 GET 和能力 POST 仍使用原 token，Last-Event-ID 不进入 POST。stop 撤销更早未完成的 start/resume；身份、reconnect、fresh start 使旧游标请求失效，不重定向到新资源。控制在开流安排后返回 Opening，流的开启期限由 driver 约束；排队取消/超时不发 GET，安排后及时观察到的取消会 join 新 I/O，已完成操作不回滚。

QueueOptions.notification_recovery 默认 None；NotificationRecoveryPolicy 只在监听已开启时作用于可恢复的旧版 GET。限制 1..8 次、backoff <=60s、总窗口 (0,600s]，每次遵守 server retry 与 backoff 较大值。从 reader 报告断线起计时，owner 等待、排空能力回复和开启都消耗同一窗口，开流 timeout 收紧到剩余窗口。连续 EOF 共用预算，reader 实际打开/结束时间证明持续健康达到窗口后才重置；串行元数据等待既不能延后首次预算起点，也不能把已结束的 reader 误算为健康。预算耗尽保留资源失败诊断，显式手动成功可开始新一轮；自动尝试不伪造用户 queue operation。

无/重置/不安全游标、完整坏消息、认证/session/endpoint 拒绝及能力回复 writer 失败均阻止自动恢复。stop 关闭自动监听/恢复；取消手动操作不会修改另行 opt-in 的策略。等待 provider 时继续喂通知、写能力回复、恢复时钟和其他调用响应。串行元数据/reconnect 仍在安全 owner 边界处理；同步工作不被抢占。业务 POST、初始化和失败调用不在这条恢复路径重放，也未实现 session 自动续建。

真实 HTTP 专项覆盖 16 路满载、慢回复、同身份 token 轮换、provider 等待中的手动/自动恢复、预留名额、取消/到期/stop、身份/reconnect 边界、持续 EOF 次数/总窗口、健康重置及开启超时、writer 失败、单次生命周期终态和 retained result 不保留 provider。另使用 rmcp 3.10.0 的真实 LocalSessionManager 缓存：代理仅截断首个完整 SDK priming，通知由 SDK 在断流后缓存，手动/自动 Last-Event-ID GET 均收到缓存通知，实际只有一次 tools/call、一次 initialize 和一次 DELETE，游标/session 未改写。WASI 接入见后文，其余完整方案范围继续推进。

## JavaScript/Worker 业务响应恢复

Remote/WorkerRemote 的 responseRecovery 默认关闭，限制整数 maxAttempts 1..8、backoff 0..60000、timeout 1..600000（毫秒），与 notificationRecovery 分开配置。无效 Worker 配置在创建 Worker 前拒绝。对象/raw、argv 和字符串调用共享 ToolResponse I/O 状态与现有 16 路调度，旧版 tools/call 才启用跟踪；现代和元数据维持原路径。

每个原调用冻结 fetch、credential、原取消请求及 session/RPC 上下文；恢复仅换 action ID、UTF-8 Last-Event-ID 只放 GET，不重新获取 provider 或重发业务 POST/initialize。首次 reader 观察断线起计算单一窗口，并收紧到原 queue/call deadline；排空旧 reader/writer、能力回复、server retry/backoff、开启和读取均消耗窗口，后续 EOF 不重置。Core 决定能否恢复，JS 不解析或重写协议消息/游标；每块最多 64 KiB。16 个待写回复加一个 writer 的边界与原生相同，异步 writer 不阻塞 reader，也不因同一 owner 的 peer provider 等待而停止。

无/重置游标、坏完整消息、HTTP 拒绝及 writer 失败停止恢复；次数耗尽由新固定 ABI exhaust_response_resumption 报告 ResponseRecoveryExhausted，Core 验证必须是已挂起且已排空回复的调用，保留原 Unknown 错误上下文和单个终态。新 Core 测试验证 untracked/open/reply-pending 的拒绝、原 operation/queue 关联、无重放/单次事件；Wasm 和 Component adapter 继续使用严格 host ID 校验，既有 forged/非法 ID 矩阵已加入新方法。WIT 与固定 adapter 需一起重建，不生成工具专属 SDK。

最终结果先被 collect，再调用观察者；后续取消/到期保持结果，终态回复清理共用 500ms。close 保留已知结果并 abort/join 余下 I/O；结果未知时先等待 reader/writer 清理，再用原 action/RPC/credential 发 200ms 上限的旧版取消。Worker 端等待 host I/O 清理 ACK 后释放 business/input 预算；事件 ACK 暂停仍能取消当前恢复 reader。JavaScript 同步任务和读取背压仍影响调度，不声明 CPU 抢占。

专项在 Node Wasm、Node jco、跨域 Chromium Remote、WorkerRemote 的显式/Auto 八种组合各运行 25 场景，共 200 个真实 HTTP 场景；浏览器采用 HTTP/2 验证真正 16 路 mixed/满载，而非连接数有限的 HTTP/1.1 串行近似。覆盖 reply drain、160 KiB chunk 切分、原凭据/session/Unicode 游标、pending provider、次数/窗口/开启/读取超时、拒绝、默认/现代/JSON/坏帧边界、取消、结果与 close 竞争、uint64 和独立通知。另有一项真实 Worker 事件 ACK 暂停后的 GET 取消测试，服务端观察流关闭并验证 host I/O map 归零；8 组真实 GC 保留成功/耗尽/取消/关闭 Promise，并验证 owner/provider/credential 回收。专项 53 个 Worker 全部观察到退出。

官方 SDK 测试扩展为八场景：Node Wasm/jco 的低层显式宿主及高层 Remote，分别验证显式旧版在途 GET 恢复与 Auto 遇到明确版本拒绝时不降级。高层结果实际从 GET 返回，仍仅一个 tools/call 与 initialize；沿用真实 SDK priming 与后台原流排空，不推断已完成结果永久缓存。test:wasm 已收录新专项，浏览器旧 fixture 的静态模块表补入 responses.js；最低编译器的新增运行验证见文末。WASI 高层业务响应恢复、会话续建与其余完整范围继续推进。

## JavaScript/Worker 通知恢复

Remote 与 WorkerRemote 新增 resumeNotifications(resourceId, options)，复用固定 Core/Wasm/WIT API，不生成工具接口。只接受正的精确整数 ID，等待服务端 retry 和旧 reader/reply writer 完成后，用原凭据/session 发新 GET，返回新资源的 opening；UTF-8 Last-Event-ID 仅在 GET 使用。两条/512 字节的 owner 清理预留中最多一个 resume，Worker 两侧也独立计数，停止/查询不等待业务准入、provider 或事件 ACK。stop 撤销更早的 start/resume，身份切换/重新连接/新 start 不把旧 resume 转到新资源。元数据屏障不再等待清理控制，避免旧恢复等待推迟身份失效。

notificationRecovery 默认关闭；配置 maxAttempts 1..8、backoff 0..60000、timeout 1..600000（毫秒），策略本身不启用监听。短 EOF 共用次数/总窗口，健康持续整个窗口才重置；手动成功可开始新一轮。窗口从 JS reader 观察断线起计，能力回复排空与开启也消耗窗口，开启 timeout 收紧到剩余时间；定时器受 JS 同步工作与事件/读取背压影响，不提供 CPU 抢占。无需 provider 查询、不占业务槽位，不重放 tools/call 或 initialize。空/重置游标、坏消息、401/404/405 和 writer 失败停止恢复；现代完成不使用旧版恢复路径。关闭清除自动定时器和凭据快照。

共享真实 HTTP fixture 在 Node Wasm/jco/跨域 Chromium Remote/WorkerRemote 的显式旧版与 Auto 共八个组合验证：满载（Node 16 路、HTTP/1.1 浏览器四路）、延迟能力回复、原凭据/UTF-8 游标、同身份 token 轮换、等待 provider 时手动/自动恢复、单 resume 预留、取消/到期/stop、身份切换、预算耗尽后的手动覆盖、连续 EOF 次数/窗口、健康重置、开启超时、拒绝/坏消息/游标重置/writer 失败。另验证现代拒绝、暂停 Worker ACK 时 resume/status/stop 独立完成，以及保留成功/失败/取消 Promise 后仍回收 owner/provider/凭据和自动 backoff timer。专项已通过，完整共享 Web 回归证据见末尾；Rust/WASI 高层驱动未在本批改变，整体方案仍未完成。

## 高层 JavaScript 调度

Remote 的 typed/JSON、argv/字符串命令与 continuation 恢复默认共用最多 16 路；数量和 UTF-8 输入预算分别限制队列，默认 64 条、8 MiB，按原生规则计入记录费用。每次提交的 Promise 带 host operationId 和 cancel()；AbortSignal、单次 timeout 与全局 cancel() 也可用。deadline 包括排队；未派发取消/过期不发送请求。connect 仍为 FIFO 屏障；命令使用普通并发准入和独立 completion，不再读取全局 serial outcome。

每个 fetch/read 操作独立推进同一个 Rust HostRuntime，故障仅影响对应请求。command_concurrent/command_line_concurrent 复用 Rust 解析器，help 返回本地值，参数/引号错误不调用全局 fail；在其他命令仍运行时也可完成。WIT 的命令准入是 help(string)/operation(u64) variant，jco 保留 BigInt ID；动作、事件、结果和错误已迁为 record/variant，固定 JS adapter 转回共享调度器接口。取消关闭相应流；旧版另发有界取消通知，新版不发送该通知。终态先于 observer 接受，完成回调内再取消不会改写成功结果。Observer 抛错交给 onEventError，不能中断传输。并发事件带 core operation_id 与 JS queue_operation_id；pending/control 现通过独立 onOperationEvent 生命周期观察，协议序列与队列序列分别计数。

绑定 dispose 后拒绝新提交，已准入调用继续持有 handle 至清理完成。Remote.close 拒绝排队工作、取消在途 I/O、等待完成，再释放绑定和跟踪的 continuation；借入的 runtime 仍由宿主随后 free/dispose。固定 Component 适配器允许 jco 复用同一 Remote 调度器。

Node Wasm 与 jco 分别运行现代/旧版；Chromium 使用跨域 HTTP/2。真实屏障要求全部 16 个请求先到达，第 17 个等待。另测 Unicode 字节与工具名预算、queued/active 命令取消与超时、局部坏流隔离、回调异常、终态取消竞争、绑定释放、并行表单恢复与 close；未派发消息与取消通知按服务端请求记录核对。32 个 continuation 满额时按真实期限清理；恢复改变插入顺序后仍不得误释放尚未过期的句柄，该边界有单调时钟推进测试。parseSafe 同时拒绝 JS 溢出到 Infinity 的输出并保留原 JSON。同步解析或事件 gate 超时后，在真正 fetch 及接受字节前再次检查总期限；测试在 Rust 已分配操作后模拟同步耗时，验证 Timeout、零 fetch 和无孤立操作。这不是同步校验的 CPU 抢占机制。

## Web Worker 与浏览器 CLI

WorkerRemote 使用固定通用消息协议，工具名和 Schema 仍从远端发现，不生成工具专属接口。Rust runtime、目录、绑定、Schema 和分词/命令解析留在 module worker；主线程只持有消息代理和 fetch I/O 驱动，不解析 MCP。tool/validate/dispose 是异步方法；typed/JSON、argv、字符串 executeLine 与 continuation API 都可通过该入口使用。错误保留 code、message、JSON pointer issues 与需要时的原始 JSON。

主线程与 worker 两侧分别限制消息数量/UTF-8 字节，默认 64 条、8 MiB，close 不受普通队列满影响。绑定/continuation 释放有 160 条、64 KiB 的独立清理预留。Core operation、worker queue、主线程 RPC 与 host I/O ID 按各自 owner/map 关联；核心 action、wire RPC、operation、binding、continuation 已使用独立域和计数器，不能跨字段混用或推算编号。

事件 mailbox 统计待发及已发未确认记录，默认 256 条、4 MiB，保留终态空间；低优先级通知不足时丢弃并报告 source 为 worker-mailbox 的 lagged。核心丢弃另外使用 source 为 core 的报告，diagnostics 分别暴露 droppedEvents/coreDroppedEvents。新增准入与下一次流读取等待 ACK，取消/deadline/close 能打断等待；重复/过期 ACK 不重复释放预算。丢失通知允许空事件批次，每次占用一个有计费的信用槽，最多一个未确认；其间新丢失只累加，不增加消息。正常 close 返回尚未发送的丢失计数；强制终止不能取回尚未上报的 worker 状态。完整 CPU 与 allocator 预算仍待验收。

I/O 驱动在主线程，36 个槽位；每条 reader 只允许一个最多 64 KiB 的 chunk 在端口中，保留的输入 chunk 合计最多 8 MiB。响应总量 MCP 8 MiB、Wasm 加载 64 MiB。worker 的 fetch 代理在取消时等待 I/O owner 的清理 ACK，再完成调用并释放输入预算。验证中确认直接终止 worker 不足以证明浏览器 fetch 关闭，因此保留主线程 AbortController；worker 失去响应时终止它、取消对应 fetch/reader、等待清理后再拒绝剩余结果。

跨上下文期限用 timeOrigin 加单调时间换算，包含消息和排队时间；同步校验的完整工作预算仍未完成。shutdownTimeout 默认两秒，超时触发强制结束，结果可能未知，不重试业务请求。close 正常路径等待 runtime/绑定/transport 清理后确认，库负责 free；随后结束 worker。

Chromium 测试在主线程禁用 Wasm compile/instantiate，现代/旧版/两种 Auto 各运行真实 mixed typed/argv/字符串 16 路屏障，另有命令独立取消/超时、并发期间本地 help/解析错误、argv/字符串引号与不扩展 shell、Schema 指针错误、原样大整数 JSON、恢复、单次取消和 timeout。停止事件 ACK 测试有限记录/字节及终态，队列满时仍可释放绑定；故意卡住真实 worker 后，服务端必须在十秒自然响应之前观察两条流关闭。另测启动取消/加载失败、Unicode 字节预算、重复 ACK，并等待实际 worker close 事件。

examples/web-worker 提供可用的浏览器 CLI 页面：连接 HTTP MCP、字符串命令、help、取消、关闭，输出经 textContent 渲染并限制展示大小。该页已通过真实浏览器操作；新增 examples/wasi-component 提供 Node/jco 直接驱动类型化 WIT 的宿主示例；独立 WASI CLI 已另在 Wasmtime 执行，更多运行时继续保留原范围。

## 会话、凭据与交互

CredentialProvider 每次 Rust Client 预检解析一次；HTTP/stdio 的 typed、字符串/argv 与恢复准入都只解析一次，结果冻结到对应请求。普通 mutable Rust 命令也通过 call_tool_with 在一次预检内完成刷新、绑定、解析与调用。命名配置引用传入 provider；默认 env 解析器会观察 token 变化。旧目录和绑定在授权上下文变化时失效，已准入调用继续使用其原始快照。

原生驱动另外保留旧版会话与创建它的凭据。即使核心身份失效已清除 session ID，重建会话或最终关闭时仍使用旧凭据 DELETE 旧会话；不会将新身份的 token 用于旧会话清理。该过程有本地 HTTP 验证，清理失败会返回错误；不是远端副作用回滚保证。

Continuation 保留原工具参数和服务端 requestState，恢复使用新请求 ID 并原样回传状态。显式支持 form/URL elicitation，不自动回答、导航或声明 sampling/roots。非法输入可在发送前更正；派发后句柄单次消费。最多 32 个句柄、8 MiB 保留 JSON、每轮 16 个输入、16 轮及不延长的五分钟交互期限，响应输入上限 1 MiB。


## 跨宿主目录与凭据策略

目录授权身份、有效期和独占需求由 HostRuntime 的同一 CatalogPolicy 计算；Rust Client 不再维护另一份 TTL 状态。Wasm 和 WIT 暴露 endpoint、catalog-status、accept-authorization、refresh-catalog 与 set-catalog-ttl；WIT 的目录状态为类型化 record。查询和后来的时钟更新不会延长已获取目录的有效期，现代 ttlMs 收紧配置期限，旧版使用配置期限。

Direct Remote、jco adapter 与 WorkerRemote 提供 catalogTtl、异步 credentialProvider/reference、refresh 和 freshTool。每个操作只解析一次凭据，目录页与业务请求复用快照。身份变化等待在途调用排空，再失效旧绑定和 continuation；命名命令可直接绑定新身份目录。JS 同代排队显式 refresh 合并为一次刷新，其他全局合并/订阅/持久化继续待完成。

Worker 的 provider 留在主页面，专用有界消息通道返回凭据；回调不会尝试 structured clone。endpoint/reference 来自连接配置，凭据消息上限 64 KiB，非秘密身份 ID 上限 256 UTF-8 字节。忽略取消的 provider 保留唯一回调槽直到它结束，取消方及时返回，后续查询暂报 CredentialUnavailable，避免重复取消不断增长后台回调。秘密不进入目录身份、事件和默认错误。

旧版 session 保留创建凭据，身份切换或 close 使用原凭据 DELETE，清理限时 500ms，失败保留记录并报错。实际 Chromium HTTP/2 测试发现空体状态仍可能暴露空 stream，Worker I/O 桥在重建 204/205/304 Response 前取消该 stream，保证会话 204 清理正常。

新增 Rust 策略边界测试，以及 Node Wasm/jco/Chromium 的共享目录测试，覆盖 TTL、配置与服务器上限、刷新合并、Schema 失效、provider 等待期间旧调用完成、身份切换、静态 token 轮换、取消与未结束回调限额。Worker 另通过真实 HTTP fixture 验证 provider 所在 realm/reference 和旧 session 原凭据 DELETE。

## 主机句柄的精确范围与耗尽

新增 MAX_HOST_ID = 2^53 - 1，统一核心请求/操作、绑定、continuation 和原生/JS 队列 ID 的精确范围；0 保留给未关联事件。核心通过显式分配检查停止在上限，不再溢出或重用；原生多线程队列用原子 compare_exchange 循环分配同范围 ID。请求和操作分配在准入状态变更前检查，恢复准入的本地 ID 失败不会消费原 continuation。分页中途耗尽只结束当前控制操作，不发布部分目录；新的 continuation 无法分配时不会覆盖现有句柄。

Wasm 导出移除 u64→u32 强转，使用验证过的 JS Number，运行时拒绝分数、负数、0、非有限/不安全数字及字符串/布尔/null 等隐式转换。固定 TypeScript 声明仍为 number。jco adapter 接受精确 Number/BigInt 并在 u64 lowering 前检查相同范围；原始 WIT 宿主需遵守其 u64 类型契约。Worker resumeRaw 的 continuation ID 不再限于 u32。

Rust 私有状态测试实际设置大于 u32 与最后一个可用 ID，覆盖调用、绑定、continuation、分页、恢复原子性及 64 线程争用最后 16 个队列 ID。共享低层 ABI 场景在真实 Node Wasm、jco 和 Chromium 运行：给活跃 binding/operation/action/continuation 加 2^32，验证不会读取、释放、取消或完成原资源；非法 Number/类型不改变存活操作，原操作仍返回正确结果。该测试使用本地协议输入，不向业务服务器发额外调用。

核心 action、wire RPC、operation、binding、continuation 现已拆成不重叠的 owner-local 域；各域独立计数、显式耗尽，不能依赖内部 tag 或步长。宿主队列、Worker RPC/凭据/I/O ID 仍分别由其 owner/map 限定，不声明跨所有连接全局唯一。核心事件 sequence/丢失计数与目录 generation 现有显式上限；后者的耗尽/失效行为见下文。其他计数器、更广所有权竞争及完整资源预算继续保留原验收范围。

## 类型化 WIT 与直接宿主

WIT 的动作、事件、错误、结果、目录、响应头现已使用 record/variant/enum/list。HTTP 动作包含方法、URL、字段列表和 list<u8> 请求体；响应状态/字段和字节 chunk 直接传入核心。错误包含稳定 code/message 和 JSON Pointer issues；事件使用 kind variant，已知失败元数据额外暴露 typed failure，未知事件 kind 可由 extension(string) 保留。completion 是单次消费的 option<call-outcome>，区分 completed 与 needs-input，输出校验为枚举。Schema、业务数据、协议通知和未知扩展保留 JSON 文本，不生成工具 SDK。

Component 直接转换核心数据，不解析动作/结果 JSON envelope。固定 jco adapter 将 typed ABI 适配到已有 JS Remote；通过拼接保真的 JSON 文本避免业务 uint64 被 JSON.parse 舍入，Error/Pointer 转为现有 JS 契约。原始 WIT ABI 已改变，组件和宿主需一起重新构建，尚未承诺发布稳定性。

新增 HostRuntime.begin_response_headers，与 JSON 入口共用响应元数据检查。状态限制 100..599；最多 256 字段、名称/值共 64 KiB，检查字段语法并拒绝冲突 singleton，普通重复扩展字段合并。JSON header 文本另限 64 KiB。失败不打开 decoder、不终止现有请求，允许修正后继续。Wasm 与 jco adapter 同时拒绝状态整数截断/隐式转换。

examples/wasi-component 包含可复用的 typed HTTP driver 和实际可运行的 Node 命令。直接执行字节动作、16 路 I/O、读入后推进时钟、typed 事件/结果/错误、取消/期限、流回收和旧版 session DELETE；每响应 16 MiB、每次馈入最多 64 KiB。它是匿名 HTTP 的最小宿主，不替代高层队列/provider/目录策略，也不是独立 wasi:cli/wasi:http 二进制。

新增 smoke-component 直接使用生成的类型化接口，现代/旧版和各自 Auto 模式均要求 16 请求先到达服务端，再允许响应；验证 typed 错误路径、完整 uint64 原文、表单恢复、局部取消、期限的 typed failure、连接复用和会话删除。另实际启动示例子进程验证结果/退出。现有 jco 高层/浏览器矩阵继续运行；更多真实 Component 运行时、完整事件/预算/close 竞争仍待完成。

## 核心 ID 域、共享流与取消帧

每个 Remote 的 action、wire RPC、operation、binding、continuation、HTTP notification resource 采用不重叠的编码域和独立计数器。所有值仍在 MAX_HOST_ID 内，域内耗尽可能早于该统一上限，且不重用。新请求先检查 action/RPC/operation 空间再改变状态；恢复任一准入域耗尽均不消费原 continuation。分页中 action 或 RPC 域耗尽不发布部分目录。

Action 明确携带 rpc_id（通知为 None）、rpc_method、operation_id；WIT HTTP/stream record 的宿主字段改为 action-id，响应输入方法也以 action-id 命名，调用/恢复参数分别命名 binding-id/continuation-id。HTTP 解码器以 action ID 持有响应，JSON-RPC envelope 独立核对 wire ID。旧版 initialized 通知只消费动作编号，所以两个计数器不存在固定偏移关系。Machine 暴露 active_action_id/has_action/action_operation、rpc_id_for_action/action_for_rpc_id；原来混称 request 的查询名已迁移。

stdio 按 RPC ID 查询当前 action，再完成其 operation；只有已分配过的 RPC 域 ID 才可作为迟到回复忽略。错误域、未来 ID、非法 envelope 不会被当成合法响应。进度 token 使用 wire RPC ID，并通过同一映射归属 operation；action token 不会误关联。JSON-RPC method 必须为字符串，缺少 id/method 的损坏响应不再误作通知，带 method 的伪响应不能触发自动降级或业务成功。

核心同时生成可选 cancellation 帧：旧版 HTTP 和 stdio 可取消请求含原 wire ID，不复制业务参数；initialize、无 ID 通知及现代 HTTP 不带该帧。原生串行/队列、stdio writer、JS、WIT typed host 复用原凭据/headers 直接转发，不再自行构造协议取消 JSON。stdio 取消帧继续遵守原有有序写入和持帧字节预算；WIT 最小宿主新增相同的 200ms 取消清理限额。

新增域错用/HTTP 错域 envelope/共享流迟到回复/非法角色/自动降级证据测试，并把现有 in-memory driver 的测试响应更新为实际 wire ID。真实浏览器低层 ABI 验证动作/操作/RPC/绑定互不别名；原生官方 SDK、同子进程 16 路、Node/jco/Chromium/Worker 既有矩阵继续检验响应、恢复、取消和资源清理。WIT 草案新增字段/参数名，需组件与宿主同步重建；完整计划仍未完成。

## 事件批次、预算与丢失计数

Core EventQueue 默认独立限制 256 条/4 MiB，允许 4..4096 条、48 KiB..64 MiB。每次操作准入先保留一条终态的最坏费用；即使流中的 progress/log 被丢弃，已准入操作的 completed/tool_failed/failed/cancelled/needs_input/catalog_updated 仍有空间。终态占满时返回 ConsumerLagged，不发送新业务请求。EventStats 提供记录数、计费字节、终态预留、待报告丢失与 sequence；有记录、待报告丢失或预留时不能更改预算。

事件单条上限为 16 KiB 的跨宿主编码计费量，采用 JSON 编码长度两倍加 128 字节，给 payload JSON 文本与宿主元数据留下费用。测长写入有界计数器，不为测长再分配完整字符串。超大非终态 payload 改为 truncated 标记；超大失败事件保留有界 code/message 和最多四条短 JSON Pointer，完整调用结果/错误不因此截断。这是编码预算，不是 allocator/CPU 隔离。

Rust Machine/HostRuntime/Client/QueuedRemote 的排空方法返回 EventBatch { events, dropped }。丢失显式计数，排空时消费一次；空列表也能报告最后一条丢失，不再依赖 sequence 间隙推断。核心 sequence 预留包含终态需要的编号，耗尽显式报 LimitExceeded；ID 分配或自动探测失败仍归还操作终态。原生 EventStream 另有数量/字节预算，Lagged 合并核心丢失与该消费缓冲的实际丢弃，不重复计算。同步 handler 现接收 EventStreamItem，能观察核心 Lagged，但不会宣称看见后续消费缓冲的丢弃。

Wasm 固定 envelope 返回 events/dropped，payload 使用 payload_json 文本；WIT 返回 typed event-batch 与 u64 dropped，并提供 set-event-limits/event-stats。JS Remote/WorkerRemote 的 coreEventCapacity/coreEventBytes 配置核心预算。高层 JS 分别安全解析每条 payload，大整数通知改为 raw_payload/payload_error，保留元数据并继续派发后续终态，不让一条日志破坏整批事件。直接 WIT driver 使用 onEvents(batch)，保留 typed 元数据及原始 payloadJson。

新增 Rust 测试覆盖数量/字节独立限额、终态保护、空批次丢失、精确消费、UTF-8 诊断压缩、sequence/丢失耗尽、原生两级丢失不重复计数。Node Wasm/jco/Chromium 共享场景发出 200 条进度，核对保留加丢失恰好等于发送量且两条终态都到达；Worker 另核对 core/mailbox 两种来源与诊断计数。独立 mailbox 测试暂停 ACK、继续累加丢失、恢复并正常 close，验证不会增长未确认 loss-only 消息或重复计数。排队未派发及控制操作的统一事件语义仍未完成。

该批次更新实验性 Rust/JS/WIT 接口：Vec<Event>/事件数组改为 EventBatch，原生 handler 参数改为 EventStreamItem，typed driver 的观察回调改为 onEvents。组件与宿主必须同步重建；未声明稳定 ABI。

## CommandSpec 与动态补全

新增 CommandSpec/ParameterSpec，把 Schema 到 flag 的映射统一给参数解析、文本 help、结构化参数说明和补全。参数包含名称/flag、类型、必选标记、描述及枚举值；json_only 标明不能投影的属性。args、args-file、help 保留，nullable/不确定分支与非法 flag 名仍通过完整 JSON 调用；原始 Schema 校验不变。枚举候选使用字符串保留整数文本，不经过 JS Number。

同时修复原 number flag 总是转换为 f64 的精度问题：改用共享 JSON 数字解析路径，Schema type 为 number 时仍保留 i64/u64 整数字面量，超范围或非有限值在发送前拒绝。Rust 测试将 u64::MAX 的 number 枚举候选实际插入命令并核对参数/序列化文本；Node Wasm/jco/Chromium 低层 ABI 额外直接输入含原始 uint64 的 Schema，核对 CommandSpec、补全候选及发给宿主的请求正文保持原值。

纯 complete_line(descriptors, line, cursor) 读取描述，CommandSpec::complete_argv 接受已分词参数。工具名、flag、显式布尔值和原始类型枚举可补全，支持 --flag=prefix、排除已用参数与 JSON/业务参数冲突。执行和编辑复用同一 lexer；编辑时容忍未闭合引号及末尾转义，执行仍严格拒绝。结果包含已解码前缀、完整 token 的 UTF-8 起止字节及候选 value/replacement/description/kind；光标在词中间时替换其全部后缀，带空格/引号/反斜杠的插入可由真实分词器还原。输入限 64 KiB/4096 token，结果限 128 候选/64 KiB JSON，并显式标记 truncated。不读取文件、展开环境或调用业务工具。

Core Client::with_fresh_catalog 执行一次授权预检，仅在过期或身份失效后刷新，再允许闭包读取 Machine。ClientCommandExt、原生 mutable Remote 的 command_spec/complete_line 使用它；QueuedRemote 提供有取消/deadline 的目录屏障操作。纯描述快照由应用管理新鲜度；低层 HostRuntime/ABI 拒绝过期目录并返回 CatalogStale，宿主显式刷新。

Wasm 导出 JSON command_spec/complete_line，cursor 为 checked Number，拒绝溢出别名、隐式转换、非整数及非法 UTF-8 边界。WIT 使用 typed command-spec/parameter-spec/command-completion 与 candidate records；固定 adapter 映射为共享 JS 结构。Remote 与 WorkerRemote 的 commandSpec/completeLine 进入已有授权/TTL 预检、取消和期限路径；metadata 方法返回前也检查同步执行是否超过期限。新接口与业务结果 completion(operation) 分开。

原生 complete SERVER --line LINE [--cursor UTF8_BYTES] 支持指定配置或 --url（省略 SERVER），只发现目标目录；JSON 输出完整结构，NDJSON 输出 completion envelope，文本输出候选。complete --help 不加载配置、不连接服务器。complete 加入保留 alias；--url ENDPOINT call TOOL 提供通用路由，实际测试验证 complete/tools/call 这些名称的远程工具仍可调用。每次 CLI 启动重新发现，不宣称已有跨进程缓存或 shell 自动安装。

浏览器 CLI 的 Tab/Complete 按钮走 Worker 补全；一个候选直接替换，多候选用文本按钮选择。页面把 selectionStart 的 UTF-16 位置转换为 UTF-8 字节；编辑时取消旧查询，丢弃已变化输入的结果，显示结果继续有界且使用 textContent。

新增 Rust 测试验证共享投影、JSON-only、布尔/uint64 枚举、未闭合引用、词中光标、Unicode、原样 shell 文本、数量/字节限额、冷目录无 I/O。Core 本地目录读取测试验证一次 provider、TTL/Schema 切换与身份隔离且不发 tools/call；原生 queue 测试验证本地读取、待派发取消和刷新后的 Schema。Node Wasm/jco/Chromium 验证同一补全接口、元数据、非法 cursor、过期 Schema、已有调用期间的补全；Worker 四种模式与浏览器页面实际选择带空格候选。原生 CLI 验证 HTTP/stdio、零业务调用、help 离线和管理名冲突。具体 shell 适配见下节，更广终端/平台矩阵继续按原方案推进。

### Bash、Zsh 与 Fish 适配

`mcpx completions bash|zsh|fish` 离线输出固定通用模板，不读取配置/凭据、不生成工具专属代码。模板调用内部 `__complete` 路由，本地补全管理路由、全局参数和配置 alias；选定服务器后走现有授权和新鲜目录预检，仅获取描述。新增纯 `mcpx_command::complete_argv(descriptors, completed, current)` 接受含工具名的逻辑 argv，复用 CommandSpec，不把已分词内容重新当命令字符串解析。

原生和 WASI 共用规划和输出路径。每个 MCP 操作的 timeout 至多两秒，不是进程总期限或同步 Schema CPU 抢占。每次 Tab 启动独立 CLI、重新发现目录；stdio 配置会启动指定子进程，正常发现后关闭并回收。全局 `--config`/`--schema-registry` 的显式路径可读目录，最多扫描 4096 条/返回 128 项；不从业务参数推导文件读取。WASI 的路径仍需 preopen，适配脚本需要 PATH 中真实的 `mcpx` 可执行 launcher，不能仅依靠同名 shell 函数。

编辑语法支持普通引号/转义及简单管道分隔符，不执行变量、命令替换、重定向或 tilde 展开；未知表达式不提供候选。Zsh 通过其 words/PREFIX 提供逻辑 argv，移除引号后含 `$`/反引号的已完成词保守拒绝。候选按 shell 字面量插入，丢弃空值和控制字符。输入限 64 KiB/4096 token；输出限 128 条/转义后 64 KiB。Bash 使用 Readline 当前替换片段保留复合引号和 `--flag=` 前缀，不使用会改变 enum 语义的 filenames 模式。新增保留 alias `completions`/`__complete`；同名远程工具通过显式 call 仍可访问。

scripts/smoke-shells.mjs 与 shell-pty.py 在 Linux 的 Bash 5.3、Zsh 5.9、Fish 4.9.3 中真实发送 Tab 后读取最终 argv，每个 shell/后端 22 项。覆盖 HTTP 工具/参数/枚举、未闭合及复合引号、空格、Unicode、单双引号中的命令替换文本、history 字符、inline 参数、管理命令、配置文件/目录和含替换文本的工具名。最终接受命令仅记录 argv，不执行远程工具；同时要求标记文件不存在、HTTP tools/call 为零。原生 stdio 另核对 metadata 请求、EOF 及 PID 已回收。尚未扩大到词中光标/后缀、更多 shell 语法/版本或操作系统。CI 已配置安装 shell 和执行这些检查，未声称远端运行。

## 受控 SchemaRegistry 与 vocabulary 策略

新增不可变 SchemaRegistry，宿主在连接前登记 URI 与 JSON Schema 文本，或在空闲时原子替换。登记时使用 Draft 2020-12 meta-schema 验证资源语法（含未使用的定义），显式引用到数据字段的目标也检查语法；单次准备复用同一个 meta validator。编译显式使用只拒绝、不执行 I/O 的 retriever；没有登记的 HTTP/file 引用返回 SchemaReferenceUnavailable，不生成获取资源的动作，也不读文件。资源标识遵守相对 $id、retrieval URI/canonical URI 别名、锚点、递归/$dynamicRef 的基址语义；不展开 Schema。已在闭合 bundle 中登记且内容相同的工具根 $id 复用原资源，冲突内容拒绝，支持工具根参与跨资源递归。

SchemaResource 使用 uri/schema_json 固定字段，Schema 文本在 JS/WIT 中不转成业务 Number；Rust 另可从 (URI, Value) 构造。资源 URI 必须绝对且无 fragment，保留内部/官方规范标识；注册文档最多 128 个，单文档 1 MiB，JSON/URI 原文字节加每文档 128 字节不超过 8 MiB，编码 bundle 最多 16 MiB，URI/引用最多 2048 字节。Schema 图最多 16384 个节点。JSON 记录反序列化在消费数组时执行数量/源字节检查，不先收集无限记录。Stats 明确报告 source charges，不把它当作实际 allocator 占用。

策略由 URL 前缀猜测改为七个完整 vocabulary URI：core、applicator、validation、unevaluated、meta-data、format-annotation、content。未知 required vocabulary（即使使用标准前缀）和 required format-assertion 明确 SchemaUnsupported；未知 optional vocabulary 保留为扩展。format/content 保持 annotation，不联网或解码内容。所有 Schema-bearing 子节点检查方言和 vocabulary，显式引用到扩展字段的目标也审计；default/examples/const 中未作为 Schema 引用的数据不被误当作 Schema。主要 keyword 矩阵验证 $ref、组合/条件、dependentSchemas/Required、patternProperties、contains 数量、uniqueItems、unevaluatedProperties/Items 等实际拒绝行为；不宣称完整 JSON Schema conformance。

Catalog/HostRuntime 持有连接自己的 registry，身份失效时同时清空资源、绑定与 continuation；新身份必须显式重新登记。成功替换仅清除旧绑定/交互，不延长原目录 TTL；失败保持旧资源和绑定。在途 I/O 阻止低层替换，高层 queue/JS 使用可取消的目录屏障。已被应用保留的独立绑定仍可校验自身不可变快照，但 owner 在发送前拒绝过期绑定。原生 QueuedBinding 把编译完成的 ToolSchema 快照带回调用方，本地 typed builder 与 submit_json 直接使用同一组 Arc 校验器，不会因缺少外部定义失败，也无需在调用线程重新编译；传给 owner 后仍执行当前上下文检查。

原生 Client::with_schema_registry、Core Client 异步 set_schema_registry、QueuedRemote 屏障及 stats 已接通。CLI 的全局 --schema-registry FILE 显式加载 UTF-8 JSON 记录，现代/旧版 HTTP 与 stdio 均验证 input/output 引用；未登记或要求未知 vocabulary 返回配置类退出码 3。资源文件不会通过普通工具命令自动读取。

JS Remote/WorkerRemote 提供 schemaRegistry: [{ uri, schemaJson }]、setSchemaRegistry、schemaRegistryStats。主线程只处理固定字符串记录，Schema 解析、引用准备和校验仍在 worker；成功启动/终止后释放初始化配置的额外资源引用。Wasm 固定 JSON ABI 与 WIT typed schema-resource/schema-registry-stats 共用核心；jco adapter 对坏记录、超限及 JSON 语法返回一致错误，不解析 Schema 文本内部。直接 WIT driver 仅执行 MCP I/O，不获取 Schema URL。

新增 Rust 验证 URI/锚点冲突、相对/递归引用、闭合 bundle 工具根复用、大整数 const、boolean 资源、annotation 数据、无网络/file 能力、独立资源限额及 registry 更新不延长 TTL/跨身份。原生 queue 验证配置排队取消、绑定失效，以及携带引用资源的 typed/JSON 本地校验和 Send 调用。Node Wasm/jco/Chromium 共享真实服务场景验证 input/output/form 资源、失败替换原子性、新旧 Schema、旧 continuation 拒绝、身份清空/重新登记；Worker 四种模式另验证 Rust 资源处理与失败启动后的 runtime/I/O/source 释放，实际 16 个 worker 全部退出。低层 ABI 另核对坏 JSON/记录及 129 文档错误不改变当前资源。

编译后内存硬预算、完整同步 CPU/工作预算、其余原生/WIT/宿主生命周期矩阵与更多 Schema 互操作仍未完成；源字节/图限额不替代这些验收。JS 完成路径的具体资源保留验证见下文。原完整技术方案与持续目标不变。

## 校验器 LRU 与原生分配测量

每个 Catalog 独立持有默认 128 项的校验器 LRU，可配置 0..4096，0 禁用缓存保留。按校验器计数，input/output/form 共用，内容相同只编译一次。key 使用 SHA-256，包含 Draft 2020-12 policy version、受控 registry 的 URI/内容指纹和有界序列化的 Schema 指纹；每次查找仍先执行 JSON/根类型/源字节检查。registry 指纹在准备时计算，后续引用根的查找不重复遍历整个 bundle。明确的身份失效和资源替换另外清空缓存。

Catalog 的 binding 索引改为 Weak；活跃 binding/BoundArgs 保留自己的不可变规则，死亡 binding 不会无限钉住校验器。LRU eviction、缩容和 clear 只解除缓存引用，不修改 binding owner 或参数身份，已准入调用/continuation 仍可完成。相同工具的 live binding 可直接复用，不必经过 LRU；工具描述或上下文变化继续拒绝旧 owner handle。公开 ToolSchema 是 Send/Sync 的不可变编译快照，bind 创建独立参数身份；原生 queue 复用快照，核心 ToolBinding 仍保留非 Send 的 Rc 语义。

Rust Catalog/Machine/HostRuntime/Client 与 native QueuedRemote 公开 stats、capacity 和 clear；native Client 可在连接前配置容量。Wasm JSON ABI、WIT typed validator-cache-stats、jco adapter、Remote/WorkerRemote 接通同一核心。JS 高层维护操作走既有授权、队列、取消和 deadline，Worker 中仍由 Rust owner 执行。缓存本身的 clear/resize 不使活跃绑定失效。统计是累计 hit/miss/成功 compilation/LRU eviction，clear 不重置；所有计数饱和在 MAX_HOST_ID 并报告 counters_saturated，不向 JS 传入舍入值。

独立 native release example 使用 stats_alloc/System 测量真实 allocator 请求字节。释放应用 binding 后清空缓存，六种形状释放 2,164..248,829 字节，相对根源字节与 registry 源计费约 8.93..26.33 倍；JSON 原值、环境、计时及限制见 [测量报告](benchmarks/schema-memory-native.md)。此数据含缓存元数据，排除共享 registry/static、allocator 元数据与 RSS；只有一次观测，不是峰值、Wasm 等价值或通用放大上限。库没有安装全局 allocator。底层 Validator 未公开完整堆大小，缓存条数与源字节不能替代编译内存硬预算。

新增 Rust 回归覆盖精确 LRU/缩容、跨工具相同规则但参数身份隔离、跨 owner 缓存隔离、eviction/clear/禁用后活跃绑定仍有效、input/output 去重、registry 内容指纹、失败编译不缓存、诊断计数边界、form 复用及 clear 后恢复、原生 typed/JSON 快照与缓存控制。Node Wasm/jco/Chromium 和四种 Worker 协议模式共享重绑定命中、禁用重编译、资源/身份清空场景；低层 ABI 验证坏容量不隐式截断/转换；直接 WIT 在 16 个已准入调用期间禁用/清空缓存，全部结果仍完成。

## JS 操作完成后的资源释放

Remote/WorkerRemote 统一通过独立工厂建立 Promise 的 cancellation cell，完成时解除其回调。保留 settled Promise 不再通过 cancel 闭包持有整个 owner/job。完成路径在交付结果或错误后清除输入/start/transform/guard 闭包、原始 completion、凭据、HTTP action、binding、controller、signal、timer 与 promise resolver 等记录；绑定租约及预算仍先按原有语义归还，I/O 和旧版取消清理仍先结束。Promise 的 result/rejection 和 operationId 保留，完成后重复 cancel 安全；Worker 同步 post 失败也返回原来的 rejected Promise。

强制 GC 对照先观察到：默认 V8 lazy stack 未读取时仍保留 owner，提前物化后可回收；据此定位到结构化 CallSites/async stack 的保留。延迟格式化与自定义 formatter 的机制见 [V8 Stack trace API](https://v8.dev/docs/stack-trace-api)。现在拒绝交付前尝试物化 stack，保留 Error 身份、类型、code、issues、raw，且 stack getter 抛错不会替换原错误。队列准入前拒绝也走同一路径。测试不读取 rejection.stack，避免由测试读取掩盖宿主保留；应用自定义 stack formatter 或主动持有的引用仍由应用负责。

直接 Remote.close 在会话清理后（包括清理失败的 finally）关闭 CredentialResolver，移除 provider/reference/token/last-token 与 resolver 自持 pending 引用；关闭后不再调度新 provider。Worker CredentialHost.close 同样清除 provider、endpoint、post 闭包及 active 记录。忽略 abort 的应用回调仍可能自己持续工作，不能宣称已被强制中止。旧版直接 Remote 的 session DELETE 失败仍保留原凭据快照用于显式重试，成功后释放；没有为省内存丢掉清理能力。

新增 npm run test:lifecycle：Node --expose-gc 与 Chromium 默认 V8 stack 下，真实远程发现/JSON/SSE 调用，保留成功、raw、Schema 失败、unsafe-number 失败、准入前 TypeError、help、补全、刷新、排队取消、在途取消、超时、关闭后拒绝以及现代 form/resume Promise。每组现代 14 个、旧版 12 个。先保持 closed Remote 强引用并证明 provider 可回收，再解除该引用，证明仍被保留的 Promise 不再保留 Remote；正向对照同时证明 Promise 自己的 outcome 继续存活并可读取，取消在底层 runtime free 后也安全。

矩阵为 Node Wasm/jco 各两种协议、Chromium Wasm 两种协议和 Worker 显式/Auto 四种组合，共 10 个 owner；四个新增 worker 实际退出。该测试已加入 test:wasm/既有 CI 配置；既有 16 路、凭据未决、冻结 worker、浏览器 CLI 等回归继续通过。GC 可达性不等于 allocator/RSS、编译内存硬上限或所有宿主生命周期的完整验收。

## 独立 WASI CLI 与 WASI HTTP 库驱动

新增 mcpx-wasi crate，facade 以 wasi feature 导出。WasiDriver 实现同一个 Core Driver，只执行 wasi:http 动作、WASI monotonic clock 与 pollable I/O；库不创建 Tokio/runtime，不读环境、文件或打印。它使用同步 WASI poll，async trait 方法在单次 poll 内完成；run_blocking 仅服务这类同步驱动 future，Pending 明确报错，不宣称是通用 executor 或 16 路 WASI 调度器。异步并发 embedding 继续使用既有 HostRuntime/WIT 宿主接口。

HTTP 在开始发送后才写 body，按 check-write 许可分块，每块最多 64 KiB，避免宿主单块背压死锁；response future、stream subscribe 与绝对 deadline timer 一起 poll，写、headers、body 均受同一操作时限约束。每批输入前推进 core 时钟，Schema/协议解析仍属于 Rust 共享核心。流结束/错误/提前接受结果后按 child-before-parent 释放 stream/body/response；不缓冲整条响应。headers 仍经过同一个 typed 核心校验。网络错误不打印 URL、token、body 或 host error 原文。

超时先释放失败请求 I/O，core abort 后以独立 200ms 清理限额发送原 wire ID 的可选 cancellation packet，现代 HTTP 不发送旧版通知，也不重放 tools/call。旧版会话使用原凭据快照、500ms 限额 DELETE，成功/404/405 后释放。静态 credential/事件 handler 在驱动成功 close 后清除。当前 driver 没有异步 provider 切换接口，更多凭据/并发/HTTPS 与运行时矩阵仍待扩展。

mcpx-cli 现在按 target 选择 native 或 WASI app；同一个 bin/package 编译为 wasm32-wasip2 的 wasi:cli command，直接导入 wasi:http，不再依赖 Node 来执行 MCP I/O。native app 的 argument-file、Schema 文件读取、控制字符输出、outcome 与错误退出分类提取为 shared，原有命名配置/stdio/Ctrl-C 行为保留。WASI app 支持命名 HTTP 与直接 --url、两种显式协议/Auto、tools/call、工具 help、complete、JSON/argv、args-file/stdin、三种输出、timeout、显式 token-env 与 schema-registry。文件和环境只在 CLI 层、由 host preopen/env grant 提供；补全行中的 --args-file 不会被当成文件选项。命名配置的共享实现见后文；WASI 子进程与信号 Ctrl-C 仍不可用。

实际运行先发现 WASI 标准 wasi:cli/run 与旧 exit 只传 success/error，Rust ExitCode(3) 在宿主得到 1。为遵守 mcpx 已声明的详细退出码，CLI 明确导入 wasi:cli/exit.exit-with-code，Wasmtime 必须启用 -S http -S cli-exit-with-code；在调用前 flush stdio。该扩展是明确的 host requirement，没有假设所有 P2 runtime 都提供。库与既有 WIT component 不因这个 CLI 专用导入增加 process exit。构建/运行命令见 README。

Wasmtime 固定为 50.0.1（Linux x86_64 本机实际执行），安装脚本从官方 release 获取并校验固定 SHA-256。新增 test:wasi-cli 与单独 CI job，CI 尚未远端运行。smoke 在两种协议/Auto 实际运行 CLI，验证 JSON/SSE、Schema 拒绝零业务请求、help/补全、192 KiB 请求与更大分片响应、UTF-8 文件/目录授权/stdin、env 授权、外部 Schema、原始 uint64、NDJSON 进度/结果、session DELETE。Rust WASI example 验证 builder、自有 Serialize 类型、字符串/argv 与现代 form/resume；另用第一次 DELETE 503、第二次成功的真实服务验证显式 cleanup retry。401/403 不降级/重试，307 不跟随跳转；分别停在 headers 与 body 的流证明 timer 自行中断、连接关闭、旧版正确取消且无业务重放。

重试清理同时修复了 Core Client 的关闭状态：首次 close 失败/其 future 被丢弃后，新业务仍返回 TransportClosed，再次 close 真正重试 driver cleanup；成功后的多次 close 不重复执行。新增三项无 I/O driver 回归覆盖失败、driver cleanup Pending-drop、请求 abort Pending 时丢弃 close 三种路径；关闭标记在第一次 await 前设置，防止请求取消期间丢弃 close 后重开业务。实际 WASI legacy cleanup retry 验证对资源会话同样有效。该行为改动已重新跑原生和浏览器/WIT 回归，完整错误/生命周期矩阵仍不据此宣称完成。

## 共享配置、CLI 路由与 WASI 命名 HTTP

新增 mcpx-config：统一 ProtocolMode、HTTP/stdio 描述、配置版本、unknown field/alias 验证、1 MiB 文本/文件上限和显式加载。parse 不执行 I/O；load 只在存在相对 stdio working_directory 时解析配置文件 canonical parent。泛型 Configuration 支持平台 adapter，Native Config 仍为原生 ServerConfig 的具体别名，保留已有 enum constructors/connect 方法；Native enum 的 Deserialize 经公共定义转换，避免两份 TOML schema 漂移。默认 Debug 仅报告 transport，不泄漏 URL query、credential reference、program/args/env。facade config feature 可独立使用，不启用 native runtime/network。

Native/WASI CLI 改用同一个 routing 模块，统一 --config/--url 冲突、servers/tools/call/complete、别名/通用工具路由、协议/凭据覆盖与离线 help。direct tools 的多余参数在文件/网络之前拒绝；补全行中的 --args-file 不解释为文件读取。cursor 先按 u64 解析并校验 UTF-8/长度，再转 usize，因此 32 位 WASI 与 64 位 native 对超过 2^32、u64::MAX 和 Unicode 词内光标返回一致 InvalidCursor。InvalidEndpoint 统一归入配置退出码 3。Native 在参数文件/凭据处理前确定别名，未知 alias 不触发后续预检。

WASI 现在支持 --config FILE、默认 mcpx.toml、命名 HTTP 的发现/调用/工具 help/补全、global protocol override、env: 凭据引用及 --token-env 覆盖。混合配置允许离线列出 stdio；选择 stdio 执行则 CapabilityUnavailable，先于凭据、参数文件、Schema 文件及网络。其余 reference namespace 默认 CredentialUnavailable，调用方可显式覆盖或由 embedding app 自行提供凭据。所有实际请求与旧版 session DELETE 保留对应 credential。格式错误的 argument file 在凭据查询前拒绝。

实际 Wasmtime 验证发现：即使 -S cwd=/data，Rust 1.100 的 std::env::current_dir 仍报告 /，相对 std::fs::open 找不到文件，但 /data 的显式路径可读。CLI 文件入口现在读取 wasi:cli/environment.initial-cwd，显式解析相对 config/args-file/schema-registry 路径，不修改进程全局目录，不依赖环境 PWD，也不增加文件权限。共用模型没有把宿主工作目录策略塞进 MCP 核心。默认配置、相对参数与 Schema 文件在真实 guest 中均验证通过；缺少 preopen/env 仍拒绝。

新增共享 smoke-config-cli 场景，用同样配置和工具在 native/Wasmtime 运行 90 个独立进程，覆盖混合配置离线列表、严格字段/版本/UTF-8、没有权限的文件、离线 help、远程管理名称冲突、metadata 零业务调用、协议配置与覆盖、环境缺失/授权/覆盖、旧版原凭据 DELETE、默认 cwd/相对文件、外部 Schema、参数格式错误先于凭据、光标边界与坏 endpoint。Unsupported stdio 的 guest 路径验证在其他输入/凭据之前拒绝。该场景已分 native/wasi 加入对应 CI job，远端尚未执行。

新增四项公共配置 Rust 测试及一项 Native adapter 回归，包含默认 Debug 不泄漏参数/环境/URL/reference；原生 HTTP/stdio/官方 SDK/并发/生命周期原矩阵继续通过。独立 WASI 原有 58 guest suite 保留并重新执行。WASI 命名 HTTP 已实现，但不据此宣称系统凭据/OAuth、WASI 子进程/信号/异步并发或所有宿主资源矩阵已完成。

## 直接 Serialize 参数与结构化类型结果

ToolBinding::args_from 接收调用方已有 Serialize 类型，生成与 builder 相同的 BoundArgs；Core Client::call_typed 复用 call 的 owner/Schema/授权/TTL/事件/取消/期限路径，不构造命令行。它仍返回 CallOutcome，保持业务失败及 NeedsInput 显式。QueuedBinding 提供 args_from、submit_typed 与 call_typed，使用同一个不可变 ToolSchema 快照；Serialize/本地验证在调用线程完成，QueuedArgs 绑定原 lease，错误 owner 不能提交。submit_typed 的输入不要求 Send，转换结束后的 operation 可以跨线程；核心 future 仍不强加 Send。

ToolResponse::deserialize<T: DeserializeOwned> 只访问 structuredContent object，借用响应，不消费或改变 raw。先拒绝 is_error/Invalid output，再区分缺少结构化对象与目标 Rust 类型不匹配；不解析 content text，不把 Rust defaults 当作远程 Schema 的替代，不自动继续输入请求。输出类型由调用方拥有，数字/字段转换遵守其 Serde 实现；精确整数使用整数类型。自定义序列化/反序列化返回的错误不带出参数/结果原文，原始响应仍可由调用方显式检查。

原 serialize_value 为检测 NaN，先转 serde_value、再把原对象转 serde_json，相同 Serialize 被执行两次；带状态的 serializer 会改变值，并存在两次结果不同导致非有限值被转成 null 的路径。现在改为单次 checked Serializer 委托 serde_json::value::Serializer，所有 compound children/map keys/options/enums 都保留有限数检查，保留 serde_json 的 arbitrary-precision number 表示，不依赖工具代码生成或手动展开其私有 marker。移除 serde-value 中间结构和依赖。

Serde 递归深度另外限制 128，结果继续经过原 64-level JSON/numeric 范围和完整 1 MiB 参数预算。序列长度 hint 不做无限预分配，tuple hint 最多用于小量预留；最终参数编码通过有界计数 writer 测长，validate_json 不再为了测长额外分配整个 JSON Vec。真实 Value 的构建、调用方自定义 Serialize/Deserialize 代码本身和 Schema 编译/执行的完整 allocator/CPU 配额仍不由这些检查保证，原硬预算目标继续保留。

新增十项核心回归覆盖单次调用、所有 compound 非有限值、map key/忽略错误、serde rename/skip/null/enum、u64::MAX、超范围整数、完整 Schema/owner、错误脱敏、JSON/Serde depth/源字节及虚假巨大长度 hint、结构化结果与失败分类。另加 Core Client 的单次 preflight/拒绝前零 I/O/显式状态测试，和真实 Native queue 的 !Send 本地输入、跨线程 operation、lease/错误 owner/零错误业务派发验证。现有外部 Schema queue 场景也改为通过 args_from 验证快照资源复用。原生、queued、WASI Rust 示例使用新入口；WASI 另实际验证 deserialize 后的调用方结构，完整 raw outcomes 同时保留。

## 错误执行证据与重试建议

Error 新增 operation_id、queue_operation_id 两个可选标识，以及 ExecutionCertainty/RetryAdvice 枚举。serde 默认让旧错误保守解码为 unknown/none；Error::new 不猜测执行状态，纯本地校验/配置路径使用 Error::local。已分配 core 操作和 host queue 的错误分别关联各自 ID，尚未准入的不伪造编号。CheckedCallError 将 error 装箱避免扩大 Result 错误分支，完整 outcome 继续拥有原响应/continuation。

Machine 在 drain_actions 的宿主交接点记录已派发：tools/call 交接前失败为 not_sent，交接后失败为 unknown。即使宿主后来拒绝连接，也不从错误文本推断“没有执行”。元数据 discovery/refresh 错误只声明当前业务尝试未发送，不宣称之前的交互无副作用。401、RPC 拒绝、坏流、超时和取消均不能把已交接业务降为 not_sent。有效工具结果与显式 input-required outcome 才提供 result_received/needs_input 的正证据；取消尚未发送的 resume 只描述本轮。

响应解码、Driver 和原生队列先保存上下文，再消费 pending 状态，因此返回错误、completion 与核心终态事件保留相同关联。原生 Future 的队列关闭/准入失败也带 queue ID；尚未分配 core 操作时不设置 operation_id。CLI 在清理前取得 Ctrl-C 对应的活动上下文，stderr JSON 透传。原生/queued 的 ToolResponse 和 continuation 保存来源编号；受检调用及 borrowed structured deserialize 失败继续指出已收到结果，保留 raw。

WIT 用 typed enum 和 option<u64> 表达新增字段；jco/浏览器 adapter 将安全范围内 bigint 精确转为 JS number，超界不转换。Web Remote 在终态回调前记录执行证据。Worker 的错误编码保留经过校验的字段，queue_operation_id 映射回外层请求；成功回复另带只含标识/执行状态的 context，使 JS 不必解析或舍入大整数业务 JSON 就能给转换错误标记 result_received。Worker 强制终止只在已知正证据存在时保留终态，否则已投递业务为 unknown；未派发队列仍为 not_sent。错误包装复制共享取消 cause，避免多个 job 互相覆盖编号，交付前继续 materialize stack/释放 cancellation cell。

建议值为 none/correct_input/refresh_binding/refresh_authentication/explicit_policy/inspect_execution/inspect_result/resume_input。它们不是自动重试权限，不依赖自然语言消息、工具注解或“读操作”的猜测；默认不重放 tools/call。现有 WIT 处于开发版，此次 record 变化需要重建固定宿主 ABI，仍不生成工具专属接口。

新增五项 core 场景和一项 Client 场景覆盖交接前后取消、响应状态删除后的错误一致性、元数据 I/O、结果类型解码、恢复轮次、旧序列化格式、provider/driver 失败与不产生新动作的错误 driver。既有原生队列/受检调用场景增加 ID、执行证据和建议断言。真实 Node Wasm/jco/Chromium/Worker/WIT/原生 CLI/Wasmtime 测试增加同类断言，包括精确 uint64 的本地转换失败、排队与活动取消、冻结 Worker，以及 CLI 校验/认证/超时/Ctrl-C。仍不代表所有外部 driver、cleanup 或竞态的完整矩阵。

## 排队与控制操作的生命周期事件

新增不依赖 I/O/runtime 的 OperationEvents、OperationEvent/Batch/Kind。日志自行分配 owner-local 队列编号，含 queued/started/completed/needs_input/failed/cancelled；可选 core_operation_id 仅表达关联，不把未分配编号伪装成 0。每次准入先预留一项终态；queued/started 可有损，但独立 dropped 计数说明损失；终态不驱逐，消费者停读时新准入返回 ConsumerLagged。数量/编码字节预算复用既有事件队列；内部保留项不等于完整 allocator 硬限制。精确 ID/sequence 耗尽不复用，失败准入释放预留。显式 disable 清空订阅而保留 ID 计数。

原生 QueuedRemote 的 connect_with_events/spawn_with_events 返回独立 OperationEventStream；next_batch 可在其他任务持续消费，计数/字节预算独立于协议 EventStream。准入在共享同步日志内预留，所有者线程标记开始；排队取消/过期、丢弃 Future、控制操作、help、业务结果/输入请求和失败都走同一结算。多线程准入以短同步段统一编号/事件发布/入队，避免已发布接受顺序与 FIFO 处理顺序不同；16 线程回归检查唯一编号与相同接受/开始顺序。私有 ticket 在意外丢弃时产生保守 owner-stopped 失败，owner 退出关闭通道；消费者先取完现有批次，再见 None。订阅者 drop 明确取消订阅，后续任务继续运行。初次连接尚无原生队列 job，不制造队列编号。示例 queued 已运行并打印生命周期批次；两个实际 typed 调用和一个绑定控制任务均有唯一终态。

JS Remote/WorkerRemote 增加 onOperationEvent，同步回调没有额外积压缓冲；异常交给 onEventError，不控制 I/O。事件只保留有限诊断与状态，不包含 raw/arguments/credentials。队列/序列预算在暴露任务前预留；started 表示所有者开始处理（可能还在等待凭据），不能当作已经发包。Worker 以固定 owner-start 确认消息区分传递与开始，以外层请求 ID 结算；整体 Worker 失败不能把单个错误的执行证据赋给其他任务。queued 回调中取消在真正发包前结束；同步观察者消耗期限或序列预算时也不能越过最后检查。

核心新增只保留 scalar cell 的 QueueScope guard，供宿主关联新操作。新 core 操作在创建时保存队列关联，guard 结束不影响在途请求；开始下一操作时清理历史 serial 链接，保留活动与未消费完成的链接，避免持续控制调用造成增长。原生准入/控制使用该上下文，手动排空协议事件也能看到元数据所属 job；JS/jco/Worker 对应宿主在派发时关联。一次 job 可包含多个 core 操作与各自终态（例如刷新再调用），所以只在生命周期流统计“一次结算”，不用 core.is_terminal 加队列 ID 推断。一项 stdio TTL/半帧取消回归因此改为分别验证刷新与业务终态，并确认相同队列 ID、不同 core ID。

生命周期日志还在复制/序列化之前限制借入诊断（消息 512 UTF-8 字节、至多四对每路径不超过 64 字节的 pointers），不先复制巨大的错误正文/数组；调用方原始 Error 仍保留完整诊断。同时修复 oversized 错误事件截断：保留安全范围内操作编号、execution_certainty/retry_advice、稳定 code 和 bounded UTF-8 message/pointers，并标记 truncated，避免旧摘要逻辑丢掉上一批错误证据。Rust 测试覆盖停读/恢复准入/取消订阅、ID 上界、超大错误和原文排除、丢弃排队 future、活动取消、控制完成、scope 生存期与 128 次 serial 操作无历史积累。真实 Node Wasm/jco/Chromium/Worker 额外检查唯一终态、序列、queued 回调取消、observer 异常和序列耗尽零业务派发；GC 保留生命周期记录与 settled promises 时继续验证 owner/provider 回收。仍需扩大异常 owner、cleanup、更多平台和订阅竞争矩阵，不代表完整事件/资源验收结束。

## 连接重建、失败清理重试与不重放

Core Driver 增加可选 supports_reconnect/reconnect hook，默认不支持；Core Client 提供 reconnect、is_closed/is_suspended。重连在首次 await 前挂起新准入、重置连接 epoch，失败或 future 被丢弃时保持挂起；普通与 prepared 入口都拒绝发包。再次显式重连会清理先前未完成的元数据；任何在途业务均返回 Busy，最终 close 永久禁止重开。不支持 hook 的驱动在修改任何状态前返回 CapabilityUnavailable；WASI Rust driver、Queue/Reactor 的显式 HTTP 接入见后文；JS/Worker 的接入见下一节。

Machine/HostRuntime.reset_transport 保持 action/RPC/operation/绑定/continuation 计数器、时钟、TTL 策略和未消费的 concurrent completions；未取走的 serial outcome 以 Busy 保护，不能默默丢弃。目录/校验器与 continuation 失效，Auto 重置为最初偏好的现代探测；同身份 SchemaRegistry 的 prepared bundle 保留，新的授权上下文仍按既有策略清空。旧 typed owner 不因描述相同而再次有效，旧交互不可恢复。

原生 hook 重建 stdio 子进程或清理 HTTP MCP session，再按当前凭据重新发现；HTTP 仅保证逻辑会话重置。旧 session DELETE 始终使用原 credential snapshot。Driver 的 transport-closed、final-closed 与 cleanup-complete 分开，修复清理第一次失败后第二次 close 假成功的问题；失败 close 保持不可重开，但实际重试清理，配置 provider 即使清理失败也释放。传输中断不再提前结束 lifetime EventStream，只有最终 close 结束订阅。ProcessDriver 将已 join 的任务置 None，清理/reconnect future 在 writer join 与 child wait 之间被丢弃后可再次等待，不重复 poll 已完成 JoinHandle。Auto 探针路径也保留旧 process 至清理完成。

QueuedRemote.reconnect 是控制屏障，并在成功后替换共享 writer/sender/receiver，旧半帧、队列和过期响应不进入新连接。自动恢复默认关闭，通过 Client.with_stdio_reconnect(StdioReconnectPolicy) 在 queued owner factory 配置；只作用于共享 stdio scheduler，不自动恢复 mutable Remote/HTTP，更不重试 tools/call。max_attempts 1..8、backoff <=60s、总窗口 (0,600s]；仅 transport/start/timeout 允许重试，协议/认证/能力/Schema 失败不重试。首次无退避，其后有界退避；即使元数据成功后立刻 EOF，轮次和总窗口也持续累计，实际工具结果或连接稳定一个窗口后才重置，防止无限拉起。窗口覆盖连接清理、发现与退避；deadline 不能抢占同步编译，CPU/allocator 硬预算仍待完成。

所有原在途调用先以各自原错误、操作 ID 与 unknown 结算一次，不重新入队。尚在等待队列的命名命令在恢复后按新 Schema 首次准入，旧 binding/resume 拒绝；原 deadline 不重置。恢复期间仍处理排队取消/过期、输入预算、进程关闭与 shutdown。恢复失败结束 owner、关闭等待工作；目前连接级全局恢复诊断尚无独立状态订阅，未派发任务报告 TransportClosed，不能据此推断旧调用未执行。显式 mutable reconnect 可以失败后重试；queued owner 结束后的资源错误与管理接口仍需扩大验证。

真实 fixture 的现代/旧版/Auto 场景要求 16 个写出调用全部到达后进程退出，验证原 16 个结果均只失败一次、新进程只接收等待中的新命令/显式新调用；input Schema 改为整数后重新解析，旧绑定不发包。另有双向 Auto 版本切换、共享 I/O 替换、旧 continuation 拒绝、同身份外部引用 input/output 继续校验、HTTP 旧凭据 DELETE/身份变更、503 cleanup retry/provider 释放、重连 future 在清理时丢弃后重试、次数/期限/反复成功后 EOF 不无限重启、恢复时排队取消和过期、最终关闭不重开、未消费结果保留测试。它们是本机 fixture 证据，不是更多 OS、官方 SDK 崩溃恢复、所有 callback/owner panic 或完整协议恢复矩阵。

## JavaScript/Worker 显式 HTTP 重连

Remote.reconnect(options) 与 WorkerRemote.reconnect(options) 是普通队列控制屏障：等待原在途调用及回复清理后，复用新公开的 Wasm/WIT reset_transport，失效旧 binding/continuation/通知 checkpoint，关闭旧通知 I/O，以原 session credential DELETE 旧会话，再解析一次当前 provider 并发现完整新目录。现代不发旧版 DELETE，Auto 重新从现代探测；只重建逻辑 MCP 连接，不声明清空浏览器连接池。失败工具结果及 Unknown 证据不变，不重新排队或发送 tools/call。

重连的队列等待、旧清理、provider 与发现使用同一 deadline。排队阶段取消/超时不改旧连接；reset 后失败/取消则保留 suspended，普通业务/元数据被拒绝，显式 reconnect 可再次尝试，close/stop/status/release 仍可清理。重建进行中排在屏障后的命名命令保留原期限，成功后按新 Schema 首次准入；旧 typed/resume 不重新绑定。相同授权身份保留 SchemaRegistry，身份改变按已有策略清空。成功且监听仍开启时 fresh start，不带 Last-Event-ID；显式 stop 的禁用状态保留。旧 pending resume 在 reset 时取消，不等待它的旧 backoff，也不把其资源重定向到新 session。

Worker 的 reset 确认关联精确的 reconnect request ID；实际 reset 时双方失效并移除旧 binding 记录，worker 的历史绑定 ID 只用本地单调 cutoff 分类失效，不复用 ID 或推算 Core 句柄。业务结果另携带接收终态时的连接 epoch，保持原 JSON 不变；已知旧结果即使在 reset 消息后才送达，也不能把旧 continuation 加入新 WeakSet。scope reset 后失败也释放旧绑定预算，重复成功重连不累积过时 wrapper。JS 维持一次 job 一项生命周期终态，清理/provider/metadata 错误仅证明当前工具尝试 NotSent。

共享 HostRuntime.reset_transport 的 Rust 语义原已存在；本批增加无参数 Wasm 导出、typed WIT 方法与固定 Component adapter。低层方法无 I/O，独占宿主先关闭旧 reader/writer并保存 session 清理凭据；活跃请求和未消费 serial outcome 返回 Busy，未消费 concurrent completion 保留，计数器/时钟不重置。同一 host-id ABI 场景加入 Busy 无破坏、reset 后 completion 可读及旧 binding handle 拒绝，原 Core Rust 保护测试继续通过。

新专项在 Node Wasm/jco、跨域 Chromium Remote/WorkerRemote 的现代/旧版显式/Auto 共 16 种组合运行 200 场景和 16 组 retained-promise GC：同身份 registry、身份切换、16 路 HTTP/2 屏障、排队取消/期限、503 DELETE 失败重试、清理/发现取消与期限、失败 provider、通知恢复撤销/重开/保持禁用、旧 continuation、失败业务不重放、Auto 双向版本切换、唯一生命周期与 HTTP 关闭。额外 Worker 连续 132 次 reset，每次观察 worker/main binding map 清空且后续新调用成功；另暂扣一个真实 Worker 的旧交互结果，在重连后交付并验证结果保留、旧 continuation 在主线程本地拒绝。最终专项全部 56 个 Worker 观察退出。WASI 高层显式重连已在后续批次接入；自动 HTTP session 续建及原完整预算/性能/发布范围仍继续推进。



## 原生 HTTP session 自动续建

Client.with_http_session_recovery(HttpSessionRecoveryPolicy) 为原生 Client/ServerConfig/Queue 的初始 HTTP 连接、已建立连接的 mutable Remote 和 QueuedRemote 提供默认关闭的有界续建。配置为 max_attempts 1..8、backoff <=60s、总窗口 (0,600s]；mutable Remote 在下一次异步操作准入前续建，JS/Worker/WASI 的自动生命周期接入继续按完整目标推进。

触发依据由 Core 记录：某个实际携带 legacy MCP session 的请求收到 404，普通 POST、通知 GET、业务恢复 GET 都可提供证据。每个 Pending 与通知状态保存实际请求的 session 快照；旧 session 的迟到 404 可以终止它自己的调用，但不会在当前 session 已不同的情况下挂起新会话。普通 POST 在经过 header 校验后即报告 SessionExpired，不等候错误 JSON/大 body/EOF，也不让错误 body 的内容覆盖 session 语义。现代、无 session 的 404、401/403、provider 返回的同名字符串不产生这一续建依据。规则见 [旧版 session management](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports#session-management)。

Machine.http_session_expired / http_session_expired_at 为 Rust 宿主提供只读、非消费式 epoch 观察；时间使用宿主提供的单调毫秒时钟，在首次观察时保存，同一 epoch 的后续正/负回复不延后该时间。reset_transport 和授权上下文改变清除标记；低层宿主仍负责按既有 reset/清理契约重建逻辑会话。原失败业务的 Error/operation ID 与 Unknown 保留；普通 POST 的对应 404 从泛化 HttpError 改为准确的 SessionExpired，Error/WIT record shape 不变。NotSent 的 SessionExpired 提示显式策略，不能据此重放更早尝试。

调度器观察标记后停止新业务准入，退役旧通知 reader/checkpoint 并保持独立的 enabled/stop 偏好；已在途的旧调用继续独立完成、取消或到期。总恢复窗口从 Core 首次观察失效时开始，覆盖 peer drain、原凭据 DELETE、一次当前 provider 解析、完整发现及 backoff，不在每个阶段或成功目录之后重新计时。旧调用已接受的正结果仍可读取，窗口在 drain 中耗尽则结束 owner、按各自已执行证据结算旧调用，并以 TransportClosed/NotSent 拒绝尚未派发的工作。异步边界检查不能抢占同步 Schema 工作，最终关闭仍有既有 bounded cleanup grace。

调用尚处于凭据解析/metadata 刷新、尚未派发 tools/call 时，发现失效可以撤销其准备并归回 FIFO，保持原 job/输入预算/期限；命名命令首次派发时按新 Schema 绑定，旧 typed binding/continuation 仍拒绝。内部 OperationTicket 只发一次 Started 与一个终态，重复准备不会生成第二项生命周期开始。恢复期间继续取消/到期等待工作，shutdown 撤销 provider/metadata，重连中的 stop 阻止新 session 监听器恢复。每次成功续建仍保留剩余轮次和最初窗口；真实工具结果或完整健康空闲区间才重置连续预算，防止 idle 通知 404 或 metadata 成功/失效循环。续建中的传输错误/Timeout/再次 SessionExpired 可继续消耗轮次；认证、协议、Schema 与 HTTP server 拒绝不自动重试。没有重放旧工具 POST，没有新增独立连接状态订阅。

新增 19 项原生真实 HTTP 集成测试，覆盖显式/Auto、默认关闭/现代/无 session/401/503 排除、16 路 drain 与新整数 Schema、总窗口中断旧 peers、失败工具不重放、metadata 连续失效、原 session token 与当前 provider 顺序、shutdown/provider 回收、idle 通知失效、内部目录失效下未发送命令重绑定、provider 中断与单次生命周期、恢复 GET 404、等待取消/期限、provider 窗口、工具结果/健康空闲预算重置、idle 无限循环禁止、伪造 provider 错误代码、实际断流后的退避/逐会话 DELETE，以及恢复中 stop。四项共享 Core 测试另验证 POST header 即终态、首观察时间与正 peer 保留、普通/恢复 GET 的旧 session 快照、现代/无 session 排除及通知状态保留/重置。另新增一项 stock rmcp 3.10 SDK 测试，实际关闭 LocalSessionManager 中的真实会话，在 JSON/SSE 两种响应偏好下验证新 session、旧 binding 失效、原失败不重放及最终 session map 清空；没有修改 SDK 服务来伪造 404。


### 已连接 mutable Remote 的续建

默认关闭的同一策略已接入建立连接后的 mutable Remote。普通工具的 session 404 仍按原操作 ID、SessionExpired 和执行不确定性返回；下一次异步操作在准入前才续建。尚未发送业务请求的目录刷新若遇到真实 session 404，可以在当前操作内续建，参数 FnOnce 直到完整新目录发布后才执行一次。纯同步 tool/绑定读取不会暗中执行 I/O。

Core Driver 新增默认关闭的恢复钩子，并把既有串行 I/O 循环提取为 drive_host；该函数不 prepare/finish、不消费结果、不重试请求。只有原生 mutable owner 开启钩子；启动仍由初始发现流程管理，Queue 接管后关闭此钩子，避免队列屏障和 mutable 恢复同时调度同一连接。外部已 prepare 的目录/typed/named 准入路径同样支持恢复，采用重建后的凭据快照，保留原调用期限。老 typed binding、参数和 continuation 继续受 owner/epoch 检查；命名调用按新 Schema 首次准入。

恢复同步 reset 旧 transport 并置 suspended，然后才 await I/O；低层仍有在途工作或未消费串行结果时返回 Busy，不改 epoch、不挂起原 owner，也不丢弃已完成但未取走的结果。取消在恢复前已发生时不 reset；恢复过程中失败或 Future 被丢弃则保持 suspended，直到显式 reconnect 成功或 close。取消应继续 await，以驱动异步清理；直接丢弃 Future 不代表 DELETE 已完成。恢复使用原操作 CancellationToken；重试 finish 后仍恢复该令牌，退避、旧 DELETE、provider 和 metadata 共用原观察窗口与当前操作期限，后续首次业务发送只使用同一操作剩余时间。恢复阶段的新业务错误标记 NotSent；先前已发送的失败工具保持自己的 Unknown 证据。

启动、mutable 连续失效和队列接管共用剩余轮次；元数据成功不补充预算。Machine 保存最近一次实际接受工具结果的 operation ID（包括输入请求、业务 isError 和输出校验证据），transport reset 和结果消费不改变它；并发完成顺序不保证 operation ID 单调。NativeDriver 在结果接收时判断健康，重建前冻结该标记，防止旧 epoch 的保留结果被晚读取后误补充恢复预算。真实新工具结果或完整健康间隔才重置预算。

新增十五项真实 HTTP 场景测试、两项共享驱动测试及一项 stock SDK 测试。覆盖显式/Auto、更新 Schema/凭据、FnOnce、stale typed、prepared 准入、重试/退避、启动预算继承、健康间隔、低层并发与保留结果、取消前后和丢弃、DELETE/provider/metadata 超时、业务剩余期限、默认关闭/modern/stateless/401/503 排除以及无工具重放。SDK 测试使用 LocalSessionManager.close_session 实际终止 JSON/SSE 响应偏好下的已连接 session，检查失败工具次数、新目录和最终 manager 清空。

### 原生初始目录发现中的续建

原生 Client.build 在初始 refresh 失败后，仅在已配置策略且共享 Core 确认实际 session 404 时进入元数据续建。Client.connect、ServerConfig.connect 和 QueuedRemote.spawn_with 使用同一实现；没有 session 的 initialize/现代/stateless 404、原始传输故障、认证/server 拒绝或 provider 伪造的同名错误不凭空触发策略。失败的 initialized ACK 或分页 tools/list 均可触发，只有完整新目录发现后才交付 Remote。旧 partial catalog 不会混入重建后的目录。范围仍是 metadata 重建，不自动发送 tools/call。

整个启动使用连接前的原始 CancellationToken，而非 Driver.finish 更新后的单次操作令牌，修复第一轮失效后原 cancellation_handle 失效的问题。取消可以中断 DELETE、provider、目录与 30 秒退避，queued 启动 future 被丢弃时既有 StartupGuard 同样能结束后续各轮、释放 provider/回调并完成有界最终清理。mutable 启动应通过连接前取得的句柄取消并继续 await，以驱动清理；不把直接丢弃任意 raw Future 描述成已完成异步 DELETE。

初始恢复在第一次 Core 失效观察的同一时钟窗口中执行至多 max_attempts 个额外重建轮次；清理仍受 500ms grace，provider 解析一次当前身份，再发现目录，连续失效只花剩余轮次与原期限。成功时 NativeDriver 保存 deadline、remaining 与 metadata 成功的 healthy_since，queue 在开启通知资源前接管；因此启动用了最后一轮后，立即发生的 GET 失效不会得到新额度；启动目录占用的时间也不会在 live recovery 中被重置。只有既有的真实工具结果/完整健康区间可以恢复连续预算。时钟加法统一走检查后的 helper，相关传输重试条件同样共用定义。

新增十二项原生真实 HTTP 测试，覆盖 mutable/Queue × 显式/Auto × initialized ACK/首 tools/list 失效；现代/stateless/默认关闭/401/503、provider 同名错误和首次普通断流排除；多轮/退避与全部已观察 session 清理；有头无结束的 404 body；分页失效后旧目录丢弃；DELETE 失败及最终原凭据重试；DELETE/provider/新 metadata 的同一总期限；原句柄取消；丢弃 Queue 启动时中断长退避与三类等待、provider/回调释放；startup 到 live 的剩余轮次和原期限。另新增一项 stock SDK 初始连接测试，在 mutable/Queue × JSON/SSE 响应偏好四种组合中，于第一条 tools/list 到达服务前真实关闭 LocalSessionManager，会话失效的 404 仍由未修改的 SDK 返回；验证两次初始化、两次清理、目录完整后才首次执行工具以及最终 session map 清空。

## JS/Worker HTTP session 自动续建

Remote/WorkerRemote 的 httpSessionRecovery 默认关闭，maxAttempts 1..8、backoff 0..60000ms、timeout 1..600000ms。仅共享 Core 确认真实当前 legacy session 404 才启动；普通 POST、通知 GET 和在途业务恢复 GET 都可触发，现代/stateless/401/503/provider 同名错误不构成证据。初始 ACK、首目录或后续分页失效也可续建；原 partial catalog 不带入新连接，不因错误 body 没有结束而等待。

owner 的 HttpSessionRecovery 统一接管：立即暂停新派发、退役旧通知 I/O，独占准入锁并在首次观察的同一总窗口内等待既有业务结果及回复清理。失败工具仍以原 ID/SessionExpired/Unknown 结算；旧正结果保留。同步重置旧协议/owner epoch 及句柄后，旧 session 用创建凭据 DELETE，再解析一次当前 provider 并发现完整目录；Auto 仍按既有受控规则重新探测。未发送的 metadata/命名工作重回原 FIFO，保留 queue ID、输入预算、原期限和取消，只在新 Schema 上首次准入；生命周期 Started 与终态各一次。旧 typed/continuation 受既有绑定检查拒绝。

启动还受原 connect 期限和 AbortSignal 约束，跨 provider、失败目录和 30 秒退避的取消令牌不脱离原操作。普通等待工作的取消/期限不取消独立 owner 的恢复；耗尽后停止自动调度并 suspended，等待工作以 TransportClosed/NotSent 结束，显式 reconnect 可以重试。尚在 provider 中等待准入的 active 工作也立即中断；已接受结果优先于取消。stop/status/close 继续可用，恢复中的显式 stop 保留监听关闭偏好。自动恢复不伪造用户队列操作，连接级独立状态订阅仍待后续接口设计。

启动和已连接的连续失效共用剩余轮次/最初窗口；metadata 成功不补充，实际新工具结果或完整健康间隔才重置。显式 reconnect 成功只重新开始健康计时，挂起等待的时间不能误算成新连接健康，也不赠送轮次。接收标记来自 Core 的最后实际接受结果，不依赖可丢弃事件或晚消费旧结果。处于已证明失效的恢复 burst 内，真实传输失败可继续花轮次；认证、协议、Schema、server 拒绝不重试。读取 body 与释放 reader 同时失败时保留原传输证据，不能让 cancel() 的 TypeError 覆盖并提前中止合理的剩余轮次；旧 session DELETE 失败仍作为终态保留原快照待显式重试。

Wasm 新增 http_session_status，WIT 新增 typed http-session-status record/method，分别暴露非消费式 expired_at_ms 与 last_tool_result；固定 JS adapter 的 operation ID 使用十进制字符串，u64 不经过浮点数。Component 与 adapter 要一起重建。Worker 自动 reset 不借用 queue ID，而是独立严格递增 transport epoch；显式 reset 仍校验对应 reconnect 请求和一次确认。实际工具结果和绑定回复携带原 epoch；迟到旧结果可读，但迟到绑定/continuation 不在当前 epoch 复活，旧 binding 不占新代理句柄容量。

新专项入口 npm run test:http-session 已加入完整 test:wasm/既有 CI。真实 fixture 覆盖 35 项 legacy 生命周期场景与现代拒绝、初始/分页、16 路 Node 和 4 路浏览器 HTTP/1.1 drain、所有恢复阶段的总窗口、关闭/取消/退避、provider/身份与 registry、预算重置/连续耗尽、部分 body 传输失败、server 拒绝、通知 GET 与业务恢复 GET 失效、不重放和单次生命周期；保留 Promise 的 GC 检查及 Worker 迟到绑定回复另行计数。stock SDK 专项通过真实 LocalSessionManager.close_session 终止会话，由未修改的 MCP service 对原请求生成 404；Auto 明确版本拒绝仍直接返回，不把它伪装成 legacy 自动降级成功。

## WASI 显式 HTTP 重连

WasiDriver 实现 Core Driver 的可选 reconnect hook，支持 run_blocking(client.reconnect())；WasiQueue 和弱 WasiRemote 提供 reconnect() -> WasiOperation<()> 与 is_suspended。同步形式仍在单次 Future poll 内阻塞；Queue/Reactor 复用已有 pollable Authentication、provider future、Metadata/CallIo 状态机，不通过 run_blocking 驱动队列重连。FIFO 屏障等待旧调用及其取消/回复清理结束，清理旧 session 后解析一次当前凭据，再重新发现目录；不重放任何工具 POST。

排队原期限覆盖等待、DELETE、provider 和发现，DELETE 另有 500ms 上限；set_timeout 可在任一阶段收紧该原期限。同步 hook 的 DELETE 与 prepare/发现同样共用一个 I/O 期限；在协作式 I/O/owner 边界检查，不声称能抢占同步 Schema 工作。旧 session 始终用创建时的 credential snapshot 删除，清理失败保留描述用于显式重试；没有先用新 provider 身份清理旧会话。重置前取消/到期不改变 epoch；重置后失败/取消/operation future 被丢弃使普通工作挂起，已排队的命名任务也不能逃出失败边界。可以再次显式重连，或在同一队列预先排入下一次重连；本地配置与 stop/status/close 仍可用。最终关闭立即禁止重新打开，失败 close 的实际 session 清理可再次执行。

重置使旧 typed binding、交互 continuation、通知游标失效，并取消旧手动 resume；Auto 回到优先协议探测，不继承上一会话的协商版本。保留计数器和已完成的并发 operation 结果，同身份的 SchemaRegistry prepared bundle 跨重连/失败清理重试保留，授权上下文改变仍清空。重新发现后，已启用通知流用新资源打开，显式 stop 包括重连中的 stop 持续生效；挂起期间 start/resume 在本地拒绝，避免旧监听器恢复。只保存已完成结果不会让旧 continuation 再有效，也不会让弱 Reactor handle 持有 owner/provider。

新增真实 HTTP guest 覆盖四种显式/Auto 协议、16 路屏障与新整数 Schema、排队取消/到期、清理/provider/metadata 中断与失败重试、共享总期限、同身份 token 轮换及不同身份、冷启动/连续重连、零 TTL、保留结果/迟到交互、通知旧游标取消与禁用、单次生命周期终态、零等待非阻塞、Reactor 依赖工作流/销毁及 Auto 双向版本切换。官方 rmcp 3.10 JSON/SSE 的现代/旧版/Auto 六项 reconnect 场景另外验证两个实际工具调用、两次目录发现和旧版两个不同 session 的按序 DELETE；相同用例也进入配置 CA 的 embedding 矩阵。没有加入自动 HTTP session 续建或工具重试策略。

## 目录刷新合并与 Rust 目录观察

Catalog 引入只读 CatalogRevisionSource 和不可从整数伪造的 CatalogRevision，以 Arc 标识区分成功发布与上下文变化。每次完整 Catalog.replace 成功都会推进 publication，描述不变也算发布；授权/连接 epoch、SchemaRegistry 替换及已观察到的 tools/list_changed 推进 context。失败或仅收到部分分页不会发布。标记和观察者不直接引用 Catalog、Machine、客户端、provider、Schema 或结果；活动等待会暂存执行器 waker，变化、取消等待或关闭时释放，调用方任务仍可自行持有其他对象。

Core Client.refresh_if_unchanged(observed) 在任何预检前拒绝跨 owner token；每次仍独立 prepare 凭据，仅在 context 相同、已有较新完整 publication、目录没有过期/失效且本次身份未变时复用，返回 false 表示没有重复发现 I/O。原有 mutable refresh 始终刷新。QueuedRemote.refresh 在提交时捕获 token，控制屏障处理时应用相同策略；refresh_with_timeout 可设置本次独立期限，排队/取消/超时/生命周期仍按各自 job 处理。失败或取消的领先刷新不产生可共享成功，后续请求仍可刷新。新发布后提交的显式 refresh 会再次获取目录，不等同长期缓存或后台无主刷新。

Rust Client 与 QueuedRemote 的 subscribe_catalog 返回 CatalogWatcher，changed().await 给出最新 revision 以及 published/invalidated 标记；多个变化合并，不逐条缓存描述。每个 owner 最多保留 64 个 watcher，订阅从当前状态开始，没有初始事件；ID 按安全上界检查、不复用。丢弃 pending changed future 清除其 waker，保留订阅；丢弃 watcher 释放名额。唤醒及 waker 释放在 Mutex 之外执行。最终 close 在首次 await 前结束观察，失败清理也保持结束；Catalog drop 同样唤醒读者。未观察的新变化最多再交付一次，其后 None。重连只失效，不关闭观察。

这不是 MCP wire subscription。TTL 自然经过、刷新开始或失败不会产生观察事件；published 不保证快照此刻有效。Machine 的分页状态保留初始 context，期间若收到 list_changed，完整目录仍以 stale 发布并拒绝业务准入，下一次按现有新鲜度策略重新发现。JS 的显式刷新合并增加独立 context 标记，不跨 SchemaRegistry、身份或观察到的目录通知复用。JS/WIT 观察接口见下节。

新增 Rust 回归覆盖完整/失败发布、双订阅最新状态、pending future 释放执行器引用、64 名额/ID 上界/关闭、条件刷新 TTL/身份/registry/跨 owner、失败 cleanup 结束观察、分页失效。真实原生 HTTP fixture 阻塞领先刷新，证明 16 个请求只多一次 tools/list，逐个解析凭据、保留独立终态，另一个取消和短期限请求独立结算；失败/领先取消后下一请求重新发现，registry 屏障不被旧发布跨越。Node Wasm/jco/Chromium policy 测试同时验证 registry 屏障和每个刷新自己的预检。仍需更广 stdio 恢复、凭据和关闭竞争、协议订阅与其他宿主包装的完整验收。

## Web、WIT 与 Worker 的目录观察

Rust CatalogWatcher 新增 try_changed，同步返回 Poll<Option<CatalogChange>>，不留存 waker；最新变化在关闭后仍可读取一次。Wasm Runtime 和 WIT remote 各持有一个独立内部观察者，catalog_change / catalog-change 返回 pending、changed（仅 published/invalidated）或 closed，与可丢失的协议事件日志无关。Core 回归以四条事件预算实际挤掉 tools/list_changed，仍观察到失效并在 Machine drop 后读完结束；不通过扩大日志来规避丢失。

JS Remote（Wasm/jco）与 WorkerRemote 的 subscribeCatalog 返回有界 async iterator。每 owner 64 个显式订阅，每个只有最新 publication/context 标记和一个 pending next；tryNext 无更新时返回 undefined，关闭返回 done。next({signal}) 取消等待而不取消订阅或消耗变化，重复 pending 读报 Busy；close/return/for-await break 释放名额。订阅没有初始事件；关闭先读未见最新状态，再结束。观察者不存 Schema、凭据或 Remote，宿主事件日志的过滤/丢失不影响状态读取；TTL 自然流逝和失败 registry 替换不制造变化。它不会发协议订阅或后台刷新请求。

Worker 使用独立 catalog ACK 通道：平时只有一个已投递未确认记录与一个合并 pending cell，暂停普通事件 ACK 或 catalog ACK 不会增长这部分消息。ID 检查安全上界、不复用，重复/旧 ACK 不释放新记录。正常关闭将 pending cell 在关闭回复前发出，最多两条已投递记录；session 清理失败同样先发送最终状态并释放 Wasm，主线程结束读者。非正常 Worker 终止只能保留已交付状态，不能宣称读到了尚未交付的变化。

共享 Node Wasm/jco/Chromium 验证无初始事件、TTL/显式刷新、registry 与身份失效、过滤全部 catalog 日志后仍观察，以及关闭结束。Worker 的四种协议模式实际暂停 ACK、跨刷新/registry 合并再恢复；凭据场景观察身份变化；另有最终 pending + host 报告 DELETE 清理失败、冻结 Worker 终止后等待者结束。独立 JS 回归连续产生十万次变化，检查 64 名额/单等待/取消/return/关闭和 Worker 消息信用/最后编号。GC 矩阵保留已关闭 watcher 与 next Promise，同时证明 Remote/provider 可回收；不把这项证据当作完整堆上限。WIT 新增类型和方法，固定宿主需要同步重建；公开 API/全部生命周期与协议订阅验收仍未完成。

## HTTP 通知资源与原生后台监听

HostRuntime 新增显式 start/begin/push/finish/stop/abort notifications、notification_status 与 notification_replies。一个 runtime 同时只有一个通知资源，其 resource_id 使用新的独立 ID 域，现代 wire RPC ID 仍来自统一 RPC 分配器；不占普通工具调用/action/operation 位置，不把长期流伪装成工具结果。源通知通过 Machine.notify 更新目录 revision，原有 watcher 观察到即时失效；关闭、断开或失败标明目录存在缺口，下一次正常预检重新取目录。

现代请求是携带版本/方法和通用 metadata 的 subscriptions/listen POST，仅请求 toolsListChanged；首个 acknowledgment 必须属于本请求并确认工具列表类型，后续通知的精确 ID/类型受限，最终 resultType complete 也核对响应 ID 与 subscriptionId。旧版为独立 GET，仅使用该 session/version；不把 JSON-RPC response 当作 GET 通知，遇到未知 server request 产生 -32601 回复，由宿主以原凭据 POST。未知/未请求类型、错误顺序、数字 ID 被字符串或 2^32 偏移替换、重复确认和多义 envelope 都失败，不影响已有工具调用的完成记录。

原生 QueueOptions.http_notifications 默认 false。启用后队列所有者后台驱动一个独立 reader/writer，notification_status() 返回有界只读快照。HTTP 输入复用已有 16 消息/64 KiB chunk 通道；core 与 host 各限 16 条待回复，writer 单次等待最多 500ms。连接与确认使用 driver timeout，迟到的 opening 输入不能在期限后变成 listening；期限依赖所有者推进，不是 CPU 抢占。通知流不占 16 个调用槽，provider 等待期间仍可处理其输入；关闭或接受新身份后停止并 join 旧 reader/writer，再按当前已接受凭据建立新流。通知失败/正常结束都不自动重开；显式 reconnect 或新身份可重新建立，不重放业务请求。不额外轮询 credential provider。

每 chunk 最多 64 KiB，每 SSE frame 最多 8 MiB；长期流没有普通响应的累计 16 MiB 上限，300 批 keepalive 的回归实际跨过该大小。最多保留一个当前状态及有界回复；没有宣称 reqwest/JSON allocator 或 CPU 硬预算。身份变化、transport reset 和最终 Client close 使旧资源失效，旧输入不能进入新身份；Request 的 Debug 不输出 endpoint/header/body，明确给宿主的 I/O 序列化计划仍包含必要路由/session。

Wasm JSON ABI 和 typed WIT 同步增加这些接口、请求/状态类型，固定 component adapter 透传，严格拒绝句柄别名和 HTTP status 的截断/隐式转换。共享 fixture 在 Node Wasm、jco、跨域 Chromium 中各执行现代/旧版 × 显式/Auto，共 12 项真实 HTTP 场景，包含空闲失效、期间普通调用、GET capability reply、相关 final/意外 EOF、错误 ID 和 reader 关闭；全部使用固定凭据快照且不自动重放。原生四项测试另以四模式真实 16 路屏障验证通知不占槽、17 个业务调用各执行一次、身份/旧凭据 DELETE、默认关闭、405/无 ACK 只失败一次、流结束和连接回收。另覆盖 GET 发出 server request 后立刻 EOF：core 保留已接受的能力错误回复，原生 writer 异步排空；测试故意阻塞回复，要求新的工具调用先完成，避免把该清理变成队列屏障。身份变化/显式 stop 丢弃旧待回复，核心不允许新资源静默覆盖未处理回复。

该批打通 core/低层宿主和原生 queued HTTP；高层 JS/Worker、WASI 协作式驱动、并发队列及异步凭据的后续接入见下节，stdio 接入见“stdio 目录订阅”。更多 WASI executor/reactor 集成、更广过滤器、Last-Event-ID、session 404 自动续建和完整官方 SDK 通知互操作继续按完整方案推进。低层宿主必须自行冻结凭据、关闭 I/O 并推进核心；不能仅 free Wasm 就认为宿主网络已关闭。subscribeCatalog 仍是本地状态观察，不能单独替代网络监听。

## JS 与 Worker HTTP 后台通知

Remote/WorkerRemote 新增 httpNotifications:boolean，默认 false；startNotifications/stopNotifications/notificationStatus 复用相同 Rust 资源和协议校验，不在 JS 重建 MCP 协议状态机。启动控制返回 opening/listening 快照，不等待远端 ACK；构造器 timeout 覆盖打开/ACK，控制参数 timeout 覆盖排队及本地控制。成功连接、后续接受的新身份和显式重新连接可按启用状态打开监听；已完成/失败的流不会被普通调用或 refresh 自动重开，显式 start 可重开。状态/错误诊断保留核心的 phase/error_code。

通知 reader 和 legacy reply writer 均在普通调用槽外；reader 分块至 64 KiB，核心继续执行 8 MiB 帧限制。host 回复队列至多 16 条加 1 条 writer，回复 deadline 500 ms，整个流共享冻结凭据。EOF 后核心已接受的 legacy 能力错误会异步排空，慢 ACK 不挡普通调用；stop、身份切换和 close 关闭并等待旧 reader/writer。新身份的旧 session DELETE 仍用旧凭据。后台不轮询 provider，stop/status 也不调用 provider。

直接 owner 为 stop/status 单独保留 2 个槽、512 字节计费预算；不争用普通输入数量/字节、凭据 admission lock 和 event gate。Worker 前后两层另沿用有界 cleanup 消息预算。stop 只取消早于它的待启动，保留更晚提交的启动；预先取消或无效控制不改变既有监听。控制仍受自身 deadline 和生命周期 observer 保留容量约束，不宣称无条件成功。

WorkerFetch 的内部 notificationStream 标记允许现代 subscriptions/listen POST 或旧版 GET 连续读取，不再按普通响应的累计字节上限关闭通知；仍验证标记类型、版本/方法，保留共享 8 MiB 待读缓冲、每次跨 port 至多 64 KiB、36 项 I/O 资源限制和 pull/ACK。普通响应累计上限继续生效。

真实 HTTP fixture 覆盖 Node Wasm/jco、跨域 Chromium Remote/WorkerRemote 的显式/Auto 与现代/旧版，共 16 组自动监听；另有 8 组 ACK 超时/405 失败隔离、4 组默认关闭/手动启停/EOF 慢能力回复及 1 组 Worker event ACK 暂停。Node 要求全部 16 个业务请求先到达，浏览器受 HTTP/1.1 连接池约束使用 4 个，均验证占满时 stop/status 和旧启动取消；原有 HTTP/2 的 16 路浏览器屏障继续保留。Worker 各协议/模式流累计超过 9 MiB 后仍接收失效，业务调用不重放。

另有 8 组真实活跃通知的 GC 检查，保留控制 Promise、调用结果和 watcher，同时验证关闭后 provider、凭据对象和 owner 能被回收；既有生命周期矩阵也保留通知控制 Promise，现代每组现 17 个、旧版 15 个。Worker ACK 专项故意暂停普通事件确认，要求 stop/status 和独立 catalog 邮箱继续完成。新增 Worker fetch 单元回归覆盖普通累计上限、通知长流、错误标记/路由和缓冲上限。没有增加或重复计数 Rust 测试。

## WASI 协作式 HTTP 通知驱动

WasiDriver 新增 with_http_notifications(true)，默认关闭；WasiClientExt 为 Client<WasiDriver> 提供 start_notifications、stop_notifications、notification_status 和 poll_notifications(wait)。它直接驱动同一 HostRuntime 资源，不在 WASI 适配层重建 MCP 消息状态机。目录完整且当前凭据身份已经由 runtime 接受后，自动尝试打开一条 HTTP 通知流；手动启动对当前活动资源幂等，完成/失败后普通 fresh read 不自动重开。低层宿主若把 runtime 改到其他身份，取消旧 I/O 后不会用旧凭据打开新流，显式 start 返回 BindingStale。关闭或停止会丢弃 reader/writer 的 WASI 资源，再进行原凭据 session DELETE。

HTTP 写入拆为内部非阻塞 Exchange：请求创建、至多 64 KiB 的一次写、flush、等待 headers 各自推进，普通 Driver.execute 仍同步等待这些 pollable。其等待集合同时包括通知 reader、writer 及各自 deadline；16 步的服务预算保证持续通知不会使普通已就绪响应一直无法被处理。完成最后一次写后先 flush，再等待完成信号，空 body 也走同一路径。所有 OutputStream/InputStream/Pollable 在其父 OutgoingBody/IncomingBody 等资源之前释放，真实 guest 的正常/错误/超时路径均检查无资源树 trap。

通知打开和现代 ACK 使用独立 driver timeout；收到 ACK/旧版 headers 后删除该 timer，不给整条监听流套普通调用总 deadline。宿主仍可施加默认 idle timeout。读块不超过 64 KiB，SSE 帧仍有 8 MiB 上限，累计流量不设总上限。host 回复 FIFO 至多 16 条，加一条在途 writer，单条回复 500 ms；writer 失败或超时关闭其资源，不自动重试。读流 EOF 或坏帧后，核心先前接受的 legacy 能力错误和在途 writer 仍独立排空；慢 ACK 不阻塞普通工具请求。待回复尚存且本轮服务预算已用完时，加入即时 pollable，避免只等普通请求而遗忘待回复。

宿主在空闲时必须主动 poll_notifications；wait 上限为 24 小时，零表示推进一次有界服务，等待超时不等于通知失败。没有 I/O 或已经取得进展时可以提前返回。这个同步 Client 接口没有生成后台线程或 async executor；run_blocking 仍只接受单次 poll 即完成的同步驱动 future。独立的 16 路队列和异步 CredentialProvider 随后通过 WasiQueue 接入，见下节。现有独立 CLI 的单次调用继续默认不监听，需要常驻订阅的库宿主持有 owner 并推进轮询。

新增 examples/notifications.rs 与 smoke-wasi-notifications.mjs。真实 Wasmtime 屏障要求：普通工具响应尚未释放时，guest 必须先报告由通知产生的 catalog_stale，服务端才放行；另在 legacy reply ACK 被扣住时，要求新工具结果先完成，并检查 writer 未结束/超时。覆盖显式/Auto 两个时代、空闲失效、默认关闭/手动启停、监听寿命超过普通 deadline、EOF/读错后的冻结回复、writer 超时、普通调用超时不关闭监听、错 ID、坏 MIME/headers 超时/405、超过 9 MiB 的长流、超大帧隔离和关闭后的控制拒绝。CI 的 msrv/wasi-cli 两个 job 已加入该 guest 入口及全部 WASI 示例构建；远端 job 尚未实际运行。

## WASI 通知游标恢复

WasiClientExt 为同步 Client<WasiDriver> 增加 resume_notifications(resource_id) 和 with_timeout 变体，协作式等待原 reader/已接受的能力回复和 server retry，然后返回新资源 opening。WasiQueue/WasiRemote 增加返回 WasiOperation<NotificationStatus> 的同名方法，保留现有同步 start/stop/status；恢复最多占一个独立预留记录（128 计费字节），不占普通容量/输入字节或 16 路业务槽。set_timeout、取消/丢弃 Future、单次生命周期终态与 NotSent 等待错误复用现有语义。stats.queued 包含等待恢复；stop、新 start、身份变化与 owner close/drop 取消或失效旧操作，不把它重定向到新资源。drop 不发 session DELETE。无需额外 provider 解析，同身份 token 轮换后仍保留原通知凭据/session。

WasiDriver::with_notification_recovery(Option<NotificationRecoveryPolicy>) 默认关闭，策略本身不启用监听；校验 1..8 次、backoff <=60 秒、总窗口 (0,600 秒]。重试/开启 deadline 加入同一个 WASI poll 集合，宿主仍需驱动 idle owner，不生成后台线程或 executor。从 reader 观察 EOF 起计时，排空能力回复、退避/server retry 和开启共用窗口；开启 timeout 收紧到剩余时间。短 EOF 共用预算，持续健康达到窗口才重置，手动成功可开启新一轮。无/重置游标、坏消息、401/404/405、writer 失败禁止继续恢复；现代 HTTP 不走 GET 恢复，业务响应恢复由独立策略配置，见下一节；会话续建仍未完成。

新高层 Wasmtime fixture 覆盖显式/Auto 旧版各 24 个场景，加上四个现代拒绝场景，共 52 个 guest：同步 Client、16 路队列满载、异步 provider 等待、原凭据与 UTF-8 游标/POST 不泄漏游标、能力回复排空、server retry、取消/到期/stop、身份切换、Reactor await、owner close/drop 和 retained Future 释放 provider、默认关闭/策略不启用监听、连续 EOF 次数/窗口、健康重置、慢回复窗口、开启超时和不可恢复错误。policy 校验另有两项原生单元测试；不将这些真实 guest 重复计为 Rust 单测。

该专项实际捕获空 GET 输出流 flush 与立即响应竞争：旧 Exchange 在空请求 dispatch 后 flush，WASI 可返回 StreamError::Closed，令有效恢复请求错误失败；现改为空 body 在 dispatch 前 finish，不创建无用 output stream。调试输出已移除，库仍不打印。该更改也涉及空 DELETE，需通过既有 WASI/SDK/HTTPS/CLI 回归验证，最终结果见验证证据。新 npm run test:wasi-recovery 已加入 msrv/wasi-cli CI 配置，远端 CI 未执行。

## WASI 业务响应恢复

WasiDriver.with_response_recovery(Option<ResponseRecoveryPolicy>) 默认 None，验证 1..8 次、backoff <=60 秒、窗口 (0,600 秒]；同步 Client、WasiQueue 与 WasiReactor 继承同一 driver 配置，与通知恢复分别控制。只对旧版 HTTP tools/call 的 SSE 启用 Core 跟踪；现代 HTTP、元数据、JSON 和关闭策略维持原语义。原 queue/io 合并到共享 responses 模块，普通 metadata/call 仍用相同非阻塞 Exchange，原生/JS/WIT/Core 无协议改动。

CallIo 冻结原 credential、URL/session/版本、取消包和 RPC/operation 上下文，Core 保留原参数/Schema/progress 与 queue link；只通过新 action ID GET 续读，不重新解析 provider 或派发业务 POST/initialize。首次 owner reader 观察断线起计算固定窗口，收紧到原 job/call deadline；排空已接受回复、server retry/backoff、开启和读取都消耗窗口，短 EOF 不重置。GET 的 connect/first-byte/between-byte 请求期限收紧到剩余时间，retry/window/writer/terminal-cleanup timers 进入同一个 poll 集合。pollable 在替换/释放其父资源前先释放，InputStream 先于 IncomingBody drop；不产生 Tokio、后台线程或自主 executor。

reader 与有界 writer 分别推进，最多 16 个待写回复加一个 writer；队列在 peer provider Pending 时仍继续读写与恢复。业务槽与输入计费持有到最终清理。缺失/重置游标、坏帧、HTTP 认证/session/端点拒绝、writer 失败及次数耗尽结束原调用，结果前证据 Unknown。取消、超时和关闭使用原 RPC/bearer 取消包，且先 drop 读写资源；Last-Event-ID 只放 GET。取消通知最多 200ms，显式 close 继续驱动清理；drop owner 不发新请求/DELETE。

队列 Active 保存已接受的 completion，在回复排空前不释放槽位；已知结果优先于后续取消/期限，回复清理共用 500ms。close/drop 在此窗口也保留原结果，回收相关 resources/provider。忽略已知结果的过期 job deadline，避免终态清理期间忙等。同步 Client 同样保留最终结果，但仍是单次 Future poll 阻塞驱动；取消/丢弃 operation 的协作式路径由 Queue/Reactor 提供，宿主必须持续 poll。

新增 70 个 Wasmtime guest 场景：旧版显式/Auto 的同步客户端、180ms 原期限、终态慢回复；16 个 typed/字符串/argv 请求真实屏障后再释放、第 17 个 QueueFull、32 个能力回复；provider 等待和同身份 token 轮换；Reactor 依赖调用、次数/窗口/开启/读取超时、401/404/405/503、writer 拒绝、默认/JSON/游标/坏帧边界、通知共存、取消/超时/close/drop、已知结果与取消/close/drop、原样 uint64、配置先拒绝。现代显式/Auto 另验证不 GET、不沿用旧版会话。retained operation 在 owner close/drop 后保留结果且 provider Drop 已被观察。两项原生单测检查新策略的精确上下界。

官方 rmcp 3.10 专项新增四个 WASI guest：同步 Client 与 Queue 的显式旧版在途恢复和 Auto 明确版本拒绝。代理只截断前端 SDK priming、继续排空原后端流，10ms session retry、500ms 工具窗口内的原结果实际通过 GET 返回；仅一个工具 POST/initialize，session/游标/RPC 未改写。不推断已完成结果缓存；会话自动续建及其余完整方案范围仍未完成。

## WASI 并发队列与 Future 句柄

新增 WasiQueue/WasiQueueOptions/WasiOperation<T>。new 不发 I/O，接收新建 driver 和空闲、可预先配置 Schema 的 HTTP runtime；拒绝 stdio 和正在执行操作的 runtime。bind/refresh、typed/JSON/raw/BoundArgs、字符串/argv、显式 resume、补全和 command spec 返回泛型的一次性句柄，不生成工具专属接口。connect/fresh_tool 是调用 wait 的便捷入口。队列本身不要求 Send，也不创建 Tokio 或一般 async executor。

默认最多 16 个业务 RPC 同时进行，总容量 64（含等待、业务和元数据），输入计费 8 MiB、每项额外 128 字节；数量可设 1..1024，并发 1..16，字节 1 KiB..64 MiB。借用的输入先计费再克隆，JSON 计费通过有界 writer 完成，不为计费分配第二份完整 JSON；raw/string 在解析前检查长度，满队列在调用用户 Serialize 前拒绝。此预算仍不是完整堆预算。默认 timeout 60 秒并收紧到 driver timeout；set_timeout 从原提交时刻计时，只能进一步收紧，包含等待和目录 I/O。

目录发现、Auto 探测/旧版握手、分页和刷新也使用非阻塞 Exchange/CallIo 状态，poll(0) 不因等 HTTP readiness 而阻塞。需要刷新时等已有业务完成，再串行推进元数据，期间通知和取消清理继续服务。每个在途操作独立持有 headers/body 解码资源、deadline、冻结凭据与取消包。每轮服务每个业务流一次；每次接受 headers、字节或 EOF 前重新检查本操作 deadline 并推进 runtime 时钟，避免前一个流的同步解析消耗时间后接受另一个已过期响应。请求准备阶段的确定性错误标为 not_sent；一旦交给宿主 HTTP handler，则保留 unknown，后续失败不回放业务。

poll(wait) 驱动 owner；操作 Future 只读取/消费结果并登记 waker，需要宿主同时驱动 owner。wait(&mut operation) 自动推进所有已提交工作直到目标完成。结果先结算，再处理较晚取消。句柄 Drop 只登记取消、清除 waker，下一次 owner 服务关闭该单流；取消排队项不发业务请求。legacy 取消有独立的最多 16 项、各 200 ms 的清理池，不占业务槽。close 停止通知、结算未完工作、排空有界取消并 DELETE 原会话；owner Drop 不启动新 I/O，只关闭资源和结算 TransportClosed。保留的句柄只含结果/身份标记，不能保留 reactor 或凭据。

OperationEvents 为每个准入预留一条终态，元数据及工具操作在创建时使用 queue_scope 关联 queue ID；core operation/action/wire ID 继续独立。通过 drain_operation_events 消费有界生命周期日志；满预算会阻止准入。显式 await/try_result 仅消费一次；跨 owner 的 wait/set_timeout 拒绝，绑定和 continuation 仍由 Core 的 owner 检查决定。2 项原生单元测试验证 waker 单次结算/释放与未完成句柄 Drop。

新增 examples/queue.rs 和 smoke-wasi-queue.mjs。四种协议模式各运行 parallel/isolation/drop/metadata/metadata-cancel/metadata-timeout，另两种现代模式各验证 16 路显式恢复，以及两种模式各验证 UTF-8 输入预算、拒绝调用满队列的用户序列化器和跨 owner 句柄，共 28 个真实 Wasmtime guest。服务端必须先收到 16 个 mixed 请求，第 17 个保持等待；服务端先发目录变更，guest 观察到失效才允许倒序响应。目录专项用 stdin 握手证明构造与提交没有网络 I/O，并扣住 tools/list 响应，要求 poll(0) 仍返回，随后取消/超时可重试元数据。恢复专项检查排队取消不消费 continuation、同时发出 16 个恢复、保留含 NUL 的 opaque state，以及重复恢复没有第 17 次业务派发。

异步 provider、本地 Future runner、配置控制及刷新合并的后续实现见下文。仍需更多 executor/reactor adapter 和 runtime/SDK/关闭竞争。现有 Client<WasiDriver> 保持同步便捷接口，独立 CLI 继续执行单命令；队列没有把 run_blocking 改造成任意 Future executor，也不改变低层 WIT ABI。

## WASI 异步凭据与本地 Future runner

WasiQueue.set_credential_provider 接收同一 Core CredentialProvider（允许借用 self/request、非 Send），在没有等待/执行/元数据/凭据工作时配置或替换。reference 只来自该配置，须为 1..1024 UTF-8 字节；构造、配置和空闲通知不会查询 provider。每个请求只 resolve 一次，其 Credential 快照贯穿元数据及业务；resolving/authenticating 计入总容量和原输入计费，在 stats 中独立报告，已在途请求仍可接收响应。只允许一条 provider future，使用独占 Box lease 持有实现对象；future 完成/Drop 都归还 lease，借用的 provider future 被取消后可再次查询同一对象，不跨 await 持有 RefCell borrow。

取消、超时、关闭或 owner Drop 先丢弃凭据 future，再结算 not_sent 并释放输入计费。Close 释放已配置的 provider，即使调用方还持有关闭的 owner 和 settled 句柄；保留的晚到 waker 只指向独立 wake flag，不能复活查询或保留 reactor。Provider 错误的 code/message/issues 都替换为固定 CredentialUnavailable，超出 64 KiB 的 bearer 同样拒绝，凭据不进入目录键或普通事件。实现必须采用非阻塞、可安全取消的 Future；同步阻塞 poll 不会被该协作式调度抢占。

新身份先等旧调用排空，停止旧通知，再用原 Session 快照发 DELETE；该清理也通过异步 Exchange 和最多 500 ms deadline 推进。成功后清空旧 authority，接受新身份；原绑定/continuation 在业务派发前拒绝，命名操作才可查询新目录。取消/超时/失败的 DELETE 可能已在服务端执行，因此保留原 Session 快照供显式后续工作重试，同时清空旧目录、绑定和 continuation；不重新派发任何业务。相同身份只更新新请求使用的 token，旧请求/通知仍冻结原 token。新增 remember_session 仅在 Session ID/URL 变化时更新清理快照，修复同一 Session 被后续 token 覆盖的问题；原固定凭据驱动使用同一规则。

WasiQueue.run 接收一个可借用局部变量、非 Send 的 Result Future，和本 owner 的 I/O 一起驱动；直接使用该方法时，调用前提交要等待的操作。它响应 Future 的 wake 请求，空闲时及 provider pending 的等待集合最多每 10 ms 做一次协作式 readiness 检查，防止没有 owner I/O 时忙等或永远等到业务 deadline。返回值/错误按原 Future 交付，其余操作仍由 owner 管理。它不创建 task pool 或外部运行时服务；依赖外部 reactor 的 Future 需要宿主提供该服务。依赖式 async 提交见后续 WasiReactor/WasiRemote 一节，更多 executor/计时器/多任务服务集成继续保留范围。原 run_blocking 只用于同步 Client<WasiDriver> 的单次 poll 完成型 Future。

新增 examples/credentials.rs 与 smoke-wasi-credentials.mjs，共 48 个真实 guest：四模式各覆盖同身份 token 更新、带两个在途旧请求的身份切换、旧绑定拒绝、provider 取消/超时/错误/超大 token、关闭/drop/首次 poll 前关闭，共 40 项；现代两模式验证旧 continuation 拒绝；旧版两模式各验证 DELETE 取消/超时/503 失败后的显式重试，共 6 项。每组都运行带 Rc 的本地 yield Future，检查配置不查询 provider。测试分别扣住普通响应和 DELETE，要求旧请求在 provider 等待时完成，新身份在旧会话清理前零派发；检查 resolve 精确次数、endpoint/reference、返回/取消借用、晚到 wake、输入计费释放、provider Box 释放和错误脱敏。同身份更新后还用真实 server request 检查通知回复与最终 DELETE 的原凭据。

CI 两个 WASI job 与 npm 增加该专项入口，依然只代表本机证据及已写入的配置。没有扩展为 OAuth/系统凭据存储、任意外部 runtime 的 executor 或全部释放竞争完成声明。

## WASI 队列配置控制与刷新合并

SchemaRegistry 替换、registry/cache 统计、cache 容量/清空、TTL、elicitation 和 continuation 释放统一返回 WasiOperation，在 FIFO 中作为屏障等待之前的业务结束，后续工作不能跨越。控制在本地完成，不查询 provider 或触发目录预检；未连接的 owner 也可先配置。Registry 按 stats.bytes 加每项队列开销计入输入预算。取消/超时/Drop/close 仍走原有队列清理、计费与独立终态；未执行的变更不生效。TTL/elicitation/release 先前立即执行的方法也改成返回句柄，调用方须保留并 wait/run，两个既有 WASI 示例已更新。

Refresh 在提交时保存 CatalogRevision，准入仍逐操作解析凭据；只有在同一 owner/context 中发布过更新的完整目录且当前未过期时，才可复用。Core CatalogRevisionSource.published_since 公开该只读标记判断，文档明确它不代替 freshness 校验。每项刷新仍独立计费、deadline、取消和事件；取消/失败的 leader 终止自己的 I/O，后续操作可显式再获取目录，不重发业务。提交于新 publication 之后的显式 refresh 仍发新请求；身份、registry 失效或零 TTL 阻止跨上下文/过期复用。

新增 examples/controls.rs 与 smoke-wasi-controls.mjs：四协议模式各覆盖本地控制零网络/零 provider、在途调用屏障、registry 取消/超时、owner Drop、真实外部 Schema ref 替换、8 项刷新合并、新 publication 后刷新、leader 取消/失败与独立 follower 超时、registry/TTL 边界和身份切换，共 48 项；现代两模式另覆盖排队释放 continuation 后恢复零业务派发，共 50 个真实 guest。服务端请求次数验证合并与隔离；provider 计数验证每项刷新独立解析、本地控制不解析；原绑定失效与新 Schema 校验、缓存容量/清空、取消终态/计费、旧会话原凭据 DELETE 都有实际断言。Core 增加同 owner/context publication 判断测试；CI 两个 WASI job 与 npm 增加新入口。

更多 executor/reactor 集成、完整 lifecycle/预算、SDK/HTTPS/运行时与发布验收继续保留完整方案范围。

## WASI 依赖式异步工作流与弱提交句柄

WasiQueue.into_reactor 或 WasiReactor.new 转移既有 owner；remote() 返回可 clone、非 Send 的 WasiRemote 弱句柄。run 接收本地借用 Future，在 poll 根 Future 时不持有队列借用，操作提交的短借用在 await 前结束；因此可先 bind().await，再 typed/JSON/raw/字符串/argv 调用或显式交互恢复。所有入口仍复用原队列的预算、Schema、凭据、FIFO 控制、刷新合并、deadline、一次性结果和事件，没有额外无界 mailbox，也不生成工具专属 SDK。转移前已提交的操作保留 owner 身份，其他 owner 的 deadline 控制仍拒绝。

只有 reactor 保持队列强引用。保留 remote clone、settled 句柄或 runner 的晚到根 waker 不延长队列/provider 生命周期；owner Drop 结算未完成操作且不新增 I/O，显式 close 进行有界取消/会话清理，失败清理可再次调用 close。根 Future 返回/报错不会关闭 owner 或失败无关请求；被丢弃的未完成操作按原规则单独请求取消。Provider、事件和序列化等 owner 回调内重入使用 try_borrow_mut 返回 Busy，不跨 Future poll 持有 RefCell 借用或引入 borrow panic。Owner 已 Drop 的访问和已 close 的新提交返回 TransportClosed。

外部 executor 可分别 poll 根 Future、调用 reactor.poll(Duration::ZERO)，操作结算会唤醒根 Future。该真实测试发现旧零等待路径只做 get/read、没有 poll 对应 WASI readiness 时，在本机 Wasmtime 的 discovery 响应阶段停住；先对每项已有 pollable.ready() 做非阻塞检查，释放所有 child pollable 后执行原有单轮 service，修复此路径。直接同步驱动的 poll_notifications(0) 同步补齐该步骤。正等待保留原有有界 poll；零等待仍只有一轮 service、不等待 HTTP，旧 metadata 扣住响应场景继续检验可返回和取消，provider 首次 poll 前取消/关闭的语义也保留。两类 runner 共享同一本地驱动函数，仍只有协作式 10 ms readiness fallback，不提供外部 runtime 的 timer、spawn/task pool 等服务。

新增 examples/reactor.rs 与 smoke-wasi-reactor.mjs：四种协议模式各覆盖依赖式工作流、16 路/第 17 路屏障、取消/超时与 peer、根 Future 报错、owner Drop/close、转移前句柄、通知触发后继续调用、借用/非 Send 空闲 Future 与晚到 wake、仅 poll(0) 的外部 executor 和直接驱动通知，共 48 项；现代两模式各验证显式 continuation，旧版两模式各验证失败 close 后重试，共 52 个真实 guest。Provider/事件/用户 Serialize 都实际重入并断言 Busy；外部 executor 断言收到结算 wake；服务端检查精确调用、目录与取消/DELETE 次数、不重放、原凭据清理和通知资源关闭。关闭的 owner/弱句柄/结果仍在时 provider Drop 计数也须已释放。CI 两个 WASI job 和 npm 已增加入口，更多 runtime/宿主服务及完整生命周期矩阵继续保留。

## WASI 官方 SDK 与 HTTPS 信任拒绝矩阵

新增原生 examples/sdk_http_server.rs，使用已锁定 rmcp 3.10.0 的真实 StreamableHttpService/LocalSessionManager/ServerHandler，复用已有 SDK 工具逻辑；新增 WASI examples/sdk_interop.rs，通过实际 Wasmtime 驱动 WasiReactor/WasiRemote。Node smoke-wasi-sdk.mjs 的观察代理原样转发 SDK 响应，不生成业务/协议结果；超时场景只扣住一条已完成 SDK 调用的响应，要求 guest 在结果未知时仍完成 peer，业务仅执行一次。服务端通过两个独立 Tokio Barrier 等待全部 16 个调用/恢复才响应，maximum/parallel/resumes 计数与 wire 调用数一起校验真实并发。每个服务实例在 stdin 关闭后实际退出。

矩阵共 42 个 SDK server/guest：Modern、显式 Legacy、Auto-to-Modern 各两种 JSON/SSE 偏好下运行串行四输入、并行、外部 poll(0)、超时和通知，共 30 项；现代/Auto 两种模式各两种偏好另运行 16 路显式恢复、零 TTL 和明确现代版本拒绝，共 12 项。串行还检查 JSON 原文、调用方 Serialize、动态参数拒绝零派发、outputSchema 和结构化 deserialize；通知实际走 SDK 的 accepted_subscription_filter/listen/SubscriptionSink 或旧版 GET 通道，目录失效后重新绑定；原 session ID/bearer 用于 DELETE，旧版才发送请求取消通知。现代 -32022 版本拒绝不得触发 Auto 降级，旧版成功用显式 Legacy 验证；此前自有 fixture 的 Auto-to-Legacy 证据保持独立。

实际观测 SDK 默认 discovery/list 返回 ttlMs=0，因此并发专项通过公开 SDK handler API 显式配置 60000 ms，而零 TTL 专项必须出现 9 个真实 tools/list RPC。统计请求次数用观察代理的 wire 记录，不能把 handler 方法调用当作 wire RPC。现代无中间通知的终态在 JSON 偏好下实际为 application/json；progress 必须保留完整 SSE 序列。此 SDK 的旧版会话调用在 JSON 偏好下仍为 SSE，测试按实际 MIME 断言，不宣称已用官方 SDK 覆盖旧版 JSON 业务响应。

另 3 个 Modern/Legacy/Auto guest 连接仅本机自签名 HTTPS 服务，要求 TLS unknown-CA/certificate-unknown 信任告警、零 HTTP handler 请求及一次握手，无重试或降级。Wasmtime 50.0.1 默认 sender 使用内置公共根，依据为其[固定版本实现](https://raw.githubusercontent.com/bytecodealliance/wasmtime/v50.0.1/crates/wasi-http/src/default_send_request.rs)；本批没有关闭证书校验，也没有把拒绝证据当作可信 HTTPS 成功证据。后续可配置 trust 的宿主成功路径见下节，更多 runtime/SDK 继续保留。两类例子和脚本均使用现有锁定依赖，没有增加产品 crate 或改变 MCP/WIT ABI；CI msrv/wasi-cli 和 npm 增加专项入口。

## WASI 配置 CA 的 HTTPS embedding host

新增 tools/wasi-https-host 独立 native 工具 workspace/lockfile，固定 Wasmtime 50.0.1、reqwest 0.13.5、rustls 0.23.46，实现标准 WASIp2 CLI 与 WasiHttpHooks.send_request。工具要求 Rust 1.100，产品十 crate/guest 的 1.88 声明和主 workspace lockfile 不变，也不把 Wasmtime/reqwest 引入 WASI guest 依赖。CA 参数有 1 MiB 实际读取上限和 1..64 项检查；tls_certs_only 仅信任显式 bundle，证书链、有效期和主机名检查保持开启。一个 HTTPS origin 的宿主准入、禁用 proxy 环境与 redirect，guest 不继承环境或 filesystem preopen；标准 stdout/stderr/argv/时钟等由 WasiCtx 提供。

Bridge 流式传输 WasiBody 与 reqwest body，不收集完整 SSE；支持连接、首部和 between-byte timeout，出错映射为固定 WASI 错误，不把原始 reqwest 错误文本交给 guest。协议/Schema/凭据/取消仍由既有 guest 代码处理，host 只提供 HTTP/1.1 I/O。API 根据固定版本 [WasiHttpHooks](https://raw.githubusercontent.com/bytecodealliance/wasmtime/v50.0.1/crates/wasi-http/src/ctx.rs) 和本机依赖实现核对，代码不关闭任何证书验证开关。

smoke-wasi-sdk.mjs 增加 --https / npm test:wasi-https，复用原来的 SDK 断言与请求观察，前端网关改为 HTTPS、后端仍为仅 loopback 的真实 SDK HTTP 服务，业务/协议结果不改写。42 项包括明确版本拒绝；其余成功场景覆盖三种协议、JSON/SSE 偏好、typed/JSON/raw/字符串/argv、16 路调用和恢复、通知生命周期、poll(0)、TTL、超时和原 session DELETE。所有到达网关的请求必须有 encrypted socket。

新增 wasi-tls-fixture.mjs 生成独立 root/other-root 及正确、错误名称、已过期 leaf；不使用 sleep 等待证书过期，过期 leaf 通过 OpenSSL CA 指定过去的起止时间签发。另 18 项要求 Modern/Legacy/Auto 在错误 CA/名称/过期时恰好一次 TLS 告警且零 HTTP 派发；外部 origin 和明文在连接前拒绝；307 到明文目标只有原 HTTPS 请求，目标零访问。正常及异常退出都清理临时密钥/证书。总计 60 个 guest 和 42 个 SDK server，是该自定义宿主/网关的 HTTPS 证据；原版 Wasmtime CLI 的公共根 HTTPS、更多宿主/平台和完整运行时生命周期继续保持单独验收项。CI 新增 wasi-https job，在同一 1.100 宿主上分别执行 1.100/1.88 构建的 guest。

## 目录计数器边界与最低编译器验证

Catalog.generation 的成功发布先检查 next_generation，不超过 MAX_HOST_ID=2^53-1；超界返回 LimitExceeded，保留原目录、绑定、校验器缓存及 revision。Machine 的 discovery/refresh 在创建操作、预留事件与发出元数据动作前检查上界，失败不消耗 core ID 或产生假终态。最终成功发布可精确编码到事件，目录仍新鲜时可继续显式工具调用；需要下一次刷新时必须新建 owner。授权失效/连接重置不因计数器耗尽失败：它们仍清空旧状态，数值诊断在上限饱和，独立 Arc revision 继续报告每次失效，不靠饱和数字判断身份。

JS 的绑定 scope 和目录 publication 本地标记改为 Symbol 身份，避免长生命周期递增/舍入。静态凭据 identity counter 检查同一安全上界，最后一次轮换可用，后续变化在预检中失败且不提交 lastToken/epoch，恢复未变的最终 token 仍可调用；非法静态 token 不消耗编号。Worker 主线程共用相同 resolver，凭据通道继续泛化错误为 CredentialUnavailable，不泄露 token，拒绝时保留 not_sent。Node Wasm/jco/Chromium 与真实 Worker 检查最终编号、旧绑定拒绝、未派发及失败轮换后的继续使用。

原声明 Rust 1.88 现以实际 1.88.0 工具链验证，而非由 1.100 编译推断。scripts/check-msrv.sh 检查十个 workspace package 的 rust-version、native all-targets/test/CLI/examples、无默认 features facade、browser Wasm、WASI Component/CLI/example 与两类 Wasm facade；默认开发工具链不变。.github/workflows/ci.yml 新增独立 msrv job，包含 native/WASI 实际执行和命名配置矩阵；这里只表示配置已写入，未声称远端 CI 运行。

本机最低版本运行发现：1.88 的 WASIp2 std::fs::canonicalize 不可用，混合 HTTP/stdio 配置在执行 HTTP/list 之前就失败。保留现有 Config.load 的原生 canonical-parent 契约，新增 load_unresolved 复用有界 UTF-8/TOML 读取，WASI CLI 使用它跳过不能启动的 stdio 条目的工作目录解析；授权/文件读取和真正 stdio route 的 CapabilityUnavailable 不变。新增测试同时验证符号链接仍按真实目标父目录处理，未解析加载保留相对路径并继续执行 UTF-8/大小限制。修复后 1.88 独立配置矩阵通过；没有移除原生目录语义来绕开最低版本差异。

## Schema 策略 2：正则预检与首个错误

验证发现 jsonschema 0.61.2 的 iter_errors 会先收集整个错误列表，take(32) 只截断返回值。输入校验改用首个错误 validate，保留 JSON Pointer 和固定消息，不格式化输入/模式原文；完整对象约束和组合语义继续由同一 validator 判定。原生分配回归在 4096 个缺失字段上要求请求流量低于宽松的 64 KiB，防止重新收集整片平面错误；观测值为 408 字节，旧同形状路径为 1,234,810 字节。64/512/4096 字段的原始结果、环境和限制见更新后的 benchmarks/schema-memory-native.md 与 schema-policy-2-native.json。组合 Schema 的内部错误仍可能被收集，不能据此宣称完整 allocator/CPU 隔离。

Audit 增加整个登记 bundle + 当前根的 pattern/patternProperties 计费，每个表达式最多 4096 UTF-8 字节、总计 64 KiB、至多 256 次出现，含未使用 $defs；普通注解/const/enum 数据只有被显式引用为 Schema 才计入。Stats 增加 patterns/pattern_bytes，Rust/Wasm/typed WIT/jco/Worker 均透传，WIT record 已同步修改。源超限为 LimitExceeded；不支持的语法/翻译或超限 engine 形式为 SchemaUnsupported，失败 registry 替换保留旧目录/绑定。

jsonschema、jsonschema-regex 固定 0.61.2，fancy-regex 固定 0.20.2，使预检和实际编译使用相同翻译/engine。预检先检查 ECMA 语法、AST 深度 64、有限重复数 4096、有限静态展开估计 65536，再以 16384 VM 指令、近似 1 MiB delegate NFA / 1 MiB DFA cache 编译；实际 validator 同样设置 delegate 和 20000 回溯限制。预检表达式不留存 matcher，仅在审计中缓存有界的已通过源文本，cold compile 会多一次受限编译。静态估计不当作执行燃料，无界重复仍依赖输入长度/回溯；整个 validator/cache 堆预算与全 Schema 工作预算仍未完成。编译策略版本更新为 2，不能与旧编译条目混同。

Regex 回溯/engine 无法得出结论时，输入返回 ValidationLimitExceeded 和 not_sent；不能通过 not/if 的布尔探针变成成功。能在所有未知匹配结果下证明的恒真式仍有效。输出增加 Indeterminate 状态，原文保留；Rust checked-call/deserialize 返回 OutputValidationLimitExceeded，携带 result_received/inspect_result 及原操作上下文，CLI 返回 1。WIT enum、转换测试和 JS 生命周期摘要已接通，不把未能验证混称为输出不符。兼容的 validate_output Option<bool> 对未能验证仍投影 false，新增 validate_output_status 表达完整状态。

Rust 覆盖正则 UTF-8/数量/累积/根+registry 闭包、注解引用、短表达式膨胀、确定次数的零宽重复、嵌套静态展开、输入 not/if 与恒真式、输出状态/结构化反序列化/checked-call 原文与证据，以及宽错误的分配回归。共享 Wasm/jco/Chromium/Worker fixture 验证同一策略、统计、失败替换不变与 indeterminate；native/WASI 配置 CLI 增加输入零业务派发及输出未验证仍保留原文两项场景。

## 验证证据

Rust 测试包括参数和 Schema、分词与动态命令、HostRuntime、HTTP/stdio、配置、凭据、continuation、并发与生命周期。核心并发覆盖交错字节、倒序响应、结果背压、错 ID、坏流、局部/整体失败、迟到响应与独立恢复。

原生 HTTP 测试要求先收到全部 16 个请求，服务端才允许响应；第 17 个提交保持等待。另覆盖单请求取消/超时/坏流隔离、provider 等待时其他响应和事件继续完成、driver/queue 两层 deadline、并发关闭及旧版取消与 session 保留。单独的原凭据会话清理测试覆盖重建和直接关闭两种路径。

官方 SDK 固定为测试依赖 rmcp 3.10.0。真实 HTTP service 的五种现代/旧版 JSON/SSE/Auto 组合均验证普通调用、进度和每组 16 个 mixed typed/字符串/argv 队列调用；现代分支另验证每组 16 个显式表单恢复。SDK 空 data 预备 SSE 事件已有共享解码器回归。独立 SDK stdio 测试使用真实子进程，覆盖现代/旧版/Auto mixed 并发、现代 16 路恢复和 EOF 退出。此矩阵不代表完整 MCP conformance。

Node Wasm、jco 组件和 Chromium 各完成真实 16 路 fetch；服务端屏障证明全部请求先到达。浏览器使用临时证书的本地 HTTP/2 服务并测试跨域。其余 smoke 覆盖自动/显式协议、Schema 拒绝、字符串/argv、恢复、CLI 输出/退出码、Ctrl-C 回收和两个可运行 Rust 示例。

以上使用项目 fixture 及本机官方 SDK 服务，没有向真实业务服务发送工具调用。GitHub Actions 仅已配置，不能视为远端 CI 已执行。WASI 证据现包括 jco embedding 与 Wasmtime 50.0.1 的独立 CLI/库执行，仍不代表更多运行时或完整 SDK/HTTPS 互操作矩阵。JSON 预算和回溯限制也不等于完整 CPU/allocator 隔离。

最近 Rust 1.88.0 与 1.100.0 各通过 297 项行为测试和 1 项生成 WIT doctest，共 298 项，不把两个编译器重复计数为新增测试。fmt、1.100 全目标 Clippy、WASI CLI/配置/驱动/example 的 wasm32-wasip2 Clippy、两类 facade 目标检查通过。两编译器分别重建后，完整 test:wasm（含新 WIT 与 GC）、原生 CLI/库、112 个配置 CLI 进程和 58 个独立 WASI guest 均通过；每次完整 Wasm 套件仍有 21 个 Worker 实例退出。

本批新增 11 项 Rust 测试：7 项共享通知资源的协议/身份/ID/帧/EOF/预算边界，4 项原生通知网络与队列行为；ID 域回归扩展既有测试，不额外计数。Node Wasm、jco、跨域 Chromium 另完成 12 项实际 HTTP 通知场景。最低编译器完整日志为 target/mcpx-notifications-msrv.log，日常编译器为 target/mcpx-notifications-tests.log；同前缀记录最终构建、平台 smoke、fmt/Clippy 与 facade 检查。此前 watcher/GC、shell 和正则/分配证据分别保留于 target/mcpx-web-watch-*、target/mcpx-shell-*、target/mcpx-pattern-*；不把重复验证计作新增测试。

随后 JS/Worker 通知批次未修改 Rust 源码，沿用上述 298 项证据，不算新增 Rust 测试。使用现有 Rust 1.100 生成的 Wasm/Component，完整 npm run test:wasm 通过，日志 target/mcpx-js-notifications-wasm-final.log；其中原有 21 个和通知专项 10 个 Worker 共 31 个均退出。加强慢回复完成顺序检查后的通知专项也通过，日志 target/mcpx-js-notifications-enabled-final.log。该批新增 29 组 HTTP 行为场景（16 自动、8 失败、4 手动、1 ACK）和 8 组活跃通知 GC 场景，不重复计数最低编译器运行或既有矩阵。最低编译器未因本次纯 JS 修改重新构建，远端 CI 仍仅已配置。

WASI 通知驱动批次重跑原生 workspace 的 298 项 Rust 测试通过，未增加或重复计数该集合。1.88.0/1.100.0 分别构建 WASI 驱动、CLI 和两个示例，25 个新增通知 guest 在两个编译器下均实际通过；旧有 58 个 WASI CLI/库 guest 及 57 个 WASI 配置进程也分别通过。最终输出恢复为日常 1.100 编译器。1.100 的原生 workspace 全目标 Clippy、WASI 驱动/CLI/配置/全部示例 Clippy、fmt 和两个编译器的 wasi facade 检查通过。新 guest 包括 reader 已失败但能力 writer 仍活动、待回复超时后释放资源，以及 runtime 身份与固定凭据不一致时拒绝重开。共享 HTTP fixture 的 JS/Worker 行为及 GC 回归也通过，10 个专项 Worker 均退出。

对应日志前缀 target/mcpx-wasi-notifications-*：http-final/msrv-http-final 为两编译器各 25 个通知 guest，cli-final/msrv-cli 为旧 CLI/库，config-final/msrv-config 为配置；rust、native-clippy、final-clippy、facade/msrv-facade、js-regression 记录其余检查。构建使用已有锁定依赖和本机 Wasmtime 50.0.1，不构成完整 WASI executor、异步凭据、跨运行时或远端 CI 验收。

WASI 队列批次新增 2 项 Rust 生命周期测试，1.88.0 和 1.100.0 各完整通过 300 项（299 项行为 + 1 项 WIT doctest），不把两次编译器运行重复计数。两编译器构建的 28 项新队列 guest、旧有 25 项通知 guest、58 项独立 CLI/库和 57 项 WASI 配置场景均通过；最终产物恢复为 1.100。两编译器 wasi facade 检查、1.100 原生 workspace 和 WASI 全目标 Clippy、fmt 通过。共享 HTTP fixture 的 JS/Worker 通知及真实 GC 回归通过，10 个专项 Worker 退出。没有因这批纯 WASI/句柄变化重跑与其无关的整个 browser/component 构建矩阵。

新证据前缀 target/mcpx-wasi-queue-*：http-final/msrv-http 为队列，notifications-final/msrv-notifications 为独立通知，cli-final/msrv-cli、config-final/msrv-config 为旧入口，rust-final/msrv-rust 为各 300 项 Rust 测试；clippy/native-clippy、facade/msrv-facade、js-regression 记录其余检查。CI 的 msrv/wasi-cli job 增加 queue smoke，仍只有本机执行证据。新队列预算是有界项数和序列化输入计费，不扩大为完整堆/CPU、一般 executor 或任意凭据 provider 的完成声明。

WASI 异步凭据批次没有新增 Rust 单元测试；1.88.0 与 1.100.0 各完整通过既有 300 项（299 项行为 + 1 项 WIT doctest）。两编译器分别构建后，新增 48 个凭据/runner guest，以及既有 28 个队列、25 个通知、58 个独立 CLI/库和 57 个 WASI 配置场景通过。最终产物恢复为 1.100；原生 workspace 全目标与 WASI 驱动/CLI/配置/全部示例 Clippy、fmt、两个编译器的 wasi facade 检查通过。共享 fixture 的 JS/Worker 通知与 GC 回归通过，10 个专项 Worker 退出。CI 的 msrv/wasi-cli job 已加入凭据 smoke，未声称远端运行。

该批日志前缀 target/mcpx-wasi-credentials-*：http-final/msrv-http 为凭据，queue-final/msrv-queue、notifications-final/msrv-notifications、cli-final/msrv-cli、config-final/msrv-config 为既有专项，rust-final/msrv-rust 为各 300 项 Rust 测试；native-clippy/clippy、facade/msrv-facade、js-regression 记录其余检查。重复编译和执行不计作新增测试。

最终多套测试与编译同时执行时，旧版通知 normal 场景曾等待 Listening 超时；原诊断未记录实际 phase，原因尚未确定。随后仅增加 guest 的实际状态和 harness 的最后阶段诊断，没有放宽 deadline。通知单独重跑 25 项通过；再将通知 25 项、凭据 48 项和队列 28 项同时运行，三者均通过，对应 notifications-parallel、parallel、queue-parallel 日志。notifications-final 保存的是单独重跑结果，不代表首次并行执行没有失败；重跑通过也不作为根因已修复的证据。

WASI 配置控制/刷新合并批次新增 1 项 Core publication/context 测试，Rust 1.88.0 和 1.100.0 各通过 301 项（300 项行为 + 1 项 WIT doctest）；不重复计数两编译器执行。两个编译器构建的新增 50 个控制 guest、旧有 48 个凭据、28 个队列、25 个通知、58 个 CLI/库和 57 个配置场景均通过。完整 scripts/check-msrv.sh 通过，包含 native/Web/WASI/facade 编译检查；最终恢复 1.100 原生 CLI/示例与 Web/Component/WASI CLI/全部五个 WASI 示例。1.100 原生 workspace/WASI 全目标 Clippy、fmt 和 wasi-only facade 通过。共享 HTTP fixture 的 JS/Worker 通知及 GC 回归通过，10 个专项 Worker 均退出；这不是本批完整重跑 browser/WIT 互操作矩阵的声明。

该批证据前缀 target/mcpx-wasi-controls-*：http/msrv-http 为新增 50 项，credentials/msrv-credentials、queue/msrv-queue、notifications/msrv-notifications、cli/msrv-cli、config/msrv-config 为既有专项，rust/msrv 为两编译器 Rust 测试（msrv 同时包含完整最低版本 gate）；final-build、native-clippy/clippy、facade、js-regression 记录其余检查。CI 两个 WASI job 已加入 controls smoke，未执行远端 CI。之前记录的通知等待超时在本批两编译器通知回归均未复现，仍不将重跑通过表述为根因已解决。

WASI reactor 批次没有新增 Rust 单元测试；Rust 1.88.0/1.100.0 各完整通过既有 301 项（300 项行为 + 1 项 WIT doctest）。两个编译器各构建并实际通过新增 52 个 reactor guest、既有 48 个凭据、50 个配置控制、28 个队列、25 个通知、58 个 CLI/库和 57 个配置场景，不把重复编译计作新增测试。完整 scripts/check-msrv.sh 通过，最终恢复 1.100 原生 CLI/示例和 Web/Component/WASI CLI/全部六个 WASI 示例。1.100 原生 workspace 与 WASI 全目标 Clippy、fmt 和 wasi-only facade 检查通过。该批没有改 JS/Worker 或共享 fixture，也未把原有 JS/browser/SDK 证据扩大为本批完整重跑声明。

对应前缀 target/mcpx-wasi-reactor-*：http/msrv-http 为各 52 项，credentials/msrv-credentials、controls/msrv-controls、queue/msrv-queue、notifications/msrv-notifications、cli/msrv-cli、config/msrv-config 为既有专项；rust/msrv 为各 301 项 Rust 测试（msrv 包含完整最低版本 gate），final-build、native-clippy/clippy、facade 为其他检查。manual-debug 保留修复前零等待停在 discovery 的负例；manual-final 和 unpolled 分别保存修复后四协议的外部 executor 与首次 provider poll 前关闭验证，全套结果以 http/msrv-http 为准。CI 两个 WASI job 已加入 reactor smoke，未执行远端 CI。此前单次通知等待超时仍没有根因结论；本批通知回归未复现，不将该历史问题与已定位的零等待缺陷混同。

WASI 官方 SDK 批次没有新增 Rust 单元测试；Rust 1.88.0 和 1.100.0 各完整通过既有 301 项（300 项行为 + 1 项 WIT doctest）。两编译器各实际通过 45 个新 Wasmtime guest，42 个 SDK server 进程均正常退出，另外 3 项为独立 TLS 信任拒绝；不把重复编译计作新增测试。完整 scripts/check-msrv.sh 通过，最终恢复 1.100 的原生 CLI/示例及 Web/Component/WASI CLI/全部七个 WASI 示例，原生 workspace/WASI 全目标 Clippy、fmt、wasi-only facade 通过。产品 Rust 源码、协议/WIT、现有共享 fixture 和依赖锁定均未修改；未因本批增加独立示例/专项而重复执行无关的完整 browser/Worker 或旧 WASI 场景矩阵，其证据沿用前述批次。

对应日志前缀 target/mcpx-wasi-sdk-*：http-final/msrv-http 为各 45 项 SDK/TLS 专项，rust/msrv 为各 301 项 Rust 测试（msrv 包含完整最低版本 gate），tls 额外记录三种模式的证书信任告警验证，final-build、native-clippy/clippy、facade 记录其余检查。CI 的 msrv/wasi-cli job 已加入 SDK smoke，wasi-cli job 另编译 SDK native server 示例；未声称远端 CI 已运行。可信 HTTPS 成功、更多 SDK/运行时、完整性能/预算与发布验收继续按完整方案推进。

配置 CA 的 HTTPS 宿主批次没有新增产品 Rust 单元测试；1.88.0/1.100.0 各通过既有 301 项（300 行为 + 1 WIT doctest）。同一个 Rust 1.100 / Wasmtime 50.0.1 宿主分别执行两个编译器构建的 SDK server/guest，每组实际通过新增 60 个 HTTPS guest，42 个 SDK server 正常退出；不重复计数为 120 个新增场景。改动后的共享 SDK 脚本在原版 CLI HTTP/信任拒绝模式也分别通过 45 项回归。完整 MSRV gate 通过，最终恢复 1.100 产品构建产物和七个 WASI 示例；产品 Rust/协议/WIT/主依赖锁定不变。原生 workspace、WASI 全目标及独立宿主 Clippy、两 workspace 的 fmt、wasi-only facade 通过。独立 host workspace/lockfile 的较新编译器和 native TLS 依赖没有进入 guest：wasi facade 的实际依赖树不含 Wasmtime、reqwest、rustls 或 Tokio。

证据前缀 target/mcpx-wasi-https-*：http/msrv-http 为各 60 项，sdk-regression/msrv-sdk-regression 为各 45 项旧模式，rust/msrv 为各 301 项 Rust 测试（msrv 包含完整最低版本 gate）；host-build-final/host-clippy、final-build、native-clippy/clippy、facade、guest-deps 记录其他检查，serial/policy 是早期分项验证。CI 新增 wasi-https job，仍未执行远端 CI。本批只验证固定宿主及本机 HTTPS 网关，未扩大成原版 CLI 公共根 HTTPS、其他运行时/OS、通用宿主 ABI/资源隔离、完整性能或发布完成声明。

stdio 目录订阅批次新增 12 项 Rust 测试（6 项 Core、6 项真实子进程），并扩展既有官方 SDK stdio 测试的三个协议模式；Rust 1.88.0/1.100.0 各完整通过 313 项（312 行为 + 1 WIT doctest），不重复计数两次编译器执行。完整 scripts/check-msrv.sh、原生 workspace 全目标及 WASI 全目标 Clippy、fmt、wasi-only facade 通过。两个编译器构建的既有 25 个 WASI 通知 guest 和 45 个 SDK guest 均实际通过，各有 42 个 SDK server 正常退出；共享 SDK helper 的改动没有破坏 HTTP 通知或 WASI 对接。最终恢复 1.100 原生 CLI/示例、Web/Component、WASI CLI/全部七个示例，并用重新生成的产物完整通过 npm run test:wasm，包括 Chromium、Worker、jco 与真实 GC 回归。

本批证据前缀 target/mcpx-stdio-notifications-*：tests-final 为 Core/子进程/SDK 专项；rust-final、msrv 为各 313 项（后者包含完整最低版本 gate）；wasm-final 为最终 Web/Worker/Component 矩阵；wasi-final/msrv-wasi、sdk-final/msrv-sdk 为两编译器通知与 SDK guest；final-build、clippy-final、wasi-clippy、facade 为其他检查。WIT/协议版本/锁定依赖未改，CI 既有 workspace 检查自动收录新增 Rust 测试，未执行远端 CI。没有重复执行本批未改动的完整旧 WASI CLI/配置/队列/HTTPS 矩阵；其证据沿用前批。此前记录的单次 WASI 通知等待超时未在本批两编译器回归复现，仍不将其表述为根因已解决。

原生通知控制批次新增 9 项 Rust 测试（2 项 Core、4 项 HTTP、3 项真实 stdio），扩展既有官方 SDK stdio 测试为三个协议模式各两轮显式启动/变化/停止。Rust 1.88.0/1.100.0 各完整通过 322 项（321 行为 + 1 WIT doctest），不重复计数两编译器执行。完整 scripts/check-msrv.sh、原生 workspace 全目标及 WASI 全目标 Clippy、fmt 和 wasi-only facade 通过。最终恢复 1.100 原生 CLI/示例、Web/Component、WASI CLI/全部七个示例；重新生成的 Web/Component 通过完整 npm run test:wasm（含 Chromium、Worker、jco 和真实 GC），独立 WASI CLI/库 58 个 guest 实际通过。产品协议/WIT、JS/Worker 源码及依赖锁定未改；没有重复执行本批未修改的完整 WASI 队列/HTTPS/SDK guest 矩阵，其证据沿用前批。

本批证据前缀 target/mcpx-notification-controls-*：rust-final、msrv-final 为各 322 项（后者包含完整最低版本 gate），clippy-final、wasi-clippy、facade、final-build 为构建和静态检查，wasm-final、wasi-final 为最终实际运行。tests-final 保留迭代中 stdio fixture 同名目录失败，随后用进程内序号修复并由两编译器完整回归验证；不把该失败归因于产品协议。CI 原有 workspace 测试自动收录新增测试，未执行远端 CI。串行元数据/重连阶段的控制等待已明确记录；其余生命周期竞争与完整发布验收继续按技术方案推进。

旧版通知游标批次新增 8 项 Rust 测试（3 项协议、5 项 Core）；Rust 1.88.0/1.100.0 各完整通过 330 项（329 行为 + 1 WIT doctest），不重复计数两次编译器执行。完整最低版本 gate、原生 workspace 和 WASI 全目标 Clippy、fmt、wasi-only facade 通过。两个编译器构建的新增 4 个 Wasmtime guest 均实际通过；最终恢复 1.100 原生 CLI/示例、Web/Component、WASI CLI/全部八个示例。重建产物完整通过 npm run test:wasm，含新增 12 个真实 HTTP 场景、既有 Chromium/Worker/jco 与真实 GC。首轮 1.88 与最终 1.100 产物另各运行既有 25 个通知、58 个 CLI/库、45 个 SDK guest，42 个 SDK server 均正常退出。最终最低版本 gate 后又单独验证新增 4 个 guest；不把较早的旧矩阵执行混同为最后一次编译产物的重复运行。

证据前缀 target/mcpx-legacy-resume-*：rust-final、msrv-final 为各 330 项及最终最低版本 gate；clippy-final、wasi-clippy、final-build、facade 为静态检查与构建；wasm-final 为完整 JS/WIT/Worker 回归；wasi-final/msrv-wasi-final 为新增 4 个 guest；notifications-final、cli-final、sdk-final 及首轮 msrv-notifications/msrv-cli/msrv-sdk 为旧矩阵。新脚本使用原始 UTF-8 Last-Event-ID 并检查服务端实际重试间隔，Core 的边界检查另用显式单调时钟推进。产品协议版本/依赖锁定未改，WIT 增加固定方法并同步更新 adapter；这不生成工具专属接口。CI 已收录新场景，未执行远端 CI。高层自动恢复、业务响应流恢复、会话续建、更多过滤器/宿主与完整预算/性能仍继续按完整方案推进；历史的单次 WASI 通知等待超时未在本批复现，仍无根因结论。

原生通知恢复批次新增 14 项 Rust 测试（13 项 HTTP 生命周期/策略，1 项官方 SDK 的手动/自动缓存重放）；Rust 1.88.0/1.100.0 各完整通过 344 项（343 行为 + 1 WIT doctest），不重复计数两编译器执行。完整 scripts/check-msrv.sh、原生 workspace 全目标 Clippy、fmt 和 wasi-only facade 通过，最终恢复 1.100 的原生 CLI/示例及 Web/Component/WASI CLI/全部八个 WASI 示例。改动限于原生 crate、测试和文档；Core/协议/WIT、JS/WASI 驱动、锁定依赖未改，没有重复运行与本批无关的完整浏览器/Worker 或 WASI guest 矩阵，其实际运行证据沿用前批。

本批证据前缀 target/mcpx-native-resume-*：rust-final、msrv-final 为各 344 项及最终最低版本 gate，clippy-final、final-build、facade 为静态检查/构建，tests-final 和 sdk 为较早的专项运行。window-before 保留扩展回归在修复前的真实失败（首次 EOF 在元数据等待期间发生，旧实现多发一个 GET）；最终把窗口起点改为 reader 报告断线时间，首次等待和后续 EOF 预算两个分支均在两编译器完整测试中通过。reader 的 opened/ended 时间另用于健康重置，开启超时不能算作已验证的健康连接。CI 既有 workspace 测试自动收录新文件，未执行远端 CI。其余高层 JS/WASI 恢复、业务响应/会话恢复与完整方案验收继续推进。

这些仍是本机 Linux fixture/SDK 与现有浏览器/运行时证据，未扩展为跨 OS、完整协议订阅或所有恢复/关闭/异常 owner 竞争矩阵。新的 allocator 证据仅覆盖所列平面错误形状的请求分配，不是峰值堆/RSS、全 Schema CPU 燃料或编译器/缓存内存硬预算。远端 CI、完整性能/发布及其余完整方案验收仍未完成。

## 下一批工作

1. 继续扩大队列生命周期、cleanup/异常 owner、凭据/取消/重连竞争与跨平台验证，推进其余完整宿主 API。
2. 继续完整事件/ID/资源预算、更多浏览器/打包与释放竞争验证。
3. 扩大刷新合并与目录观察的宿主/竞争覆盖，推进其余 MCP 订阅过滤器与恢复、校验器内存硬预算和 CPU/工作预算、Schema 互操作与多宿主性能测量。
4. 继续 WIT 生命周期/预算验收、WASI executor/reactor 集成与更多关闭/配置竞争，扩大原版 CLI 公共根 HTTPS、SDK/运行时及平台矩阵，推进性能与发布验收，并跟进通知等待超时的诊断；其余缺口继续按完整技术方案推进。


JS/Worker 通知恢复批次仅修改共享 JavaScript driver、Worker RPC、真实 HTTP fixture、脚本和文档，未改 Rust/协议/WIT/锁定依赖。新增专项共有 145 组 HTTP 行为（17 场景 × 八宿主/协议组合、八组现代拒绝、一个 ACK 场景）及八组恢复 GC；其中 Node 16 路、浏览器 HTTP/1.1 四路，不能把浏览器场景计为 16 路。新专项全部通过，39 个新 Worker 均观察到退出。完整 npm run test:wasm 通过，包含已有低层通知/游标、调用、交互、补全、凭据、Worker/CLI 与生命周期 GC；保留成功/失败/取消 Promise 的恢复专项验证 owner/provider/凭据可回收。日志 target/mcpx-js-recovery-targeted-final.log、target/mcpx-js-recovery-wasm-final.log，入口 npm run test:remote-recovery 已加入完整 Web gate。

该批沿用上一批两编译器各 344 项 Rust 证据，不新增、不重复计数 Rust 测试，也未重跑无改动的 WASI guest 矩阵或远端 CI。专项先发现浏览器 HTTP/1.1 连接数造成 16 路 fixture 等待，改为四路后继续；身份测试实际发现元数据屏障等待旧 resume 的问题，已排除 cleanup controls 并在最终矩阵中验证；Worker 的零 retry 操作可能先于取消消息完成，因此 GC 取消场景使用有界 server retry，保留已完成操作终态优先规则。WASI 高层恢复、业务响应/会话续建及其余完整计划继续推进。


WASI 高层通知恢复批次新增两项 policy 边界 Rust 单测；Rust 1.88.0/1.100.0 各完整通过 346 项（345 单测/集成 + 一项 WIT doctest），不重复计数两编译器运行。完整 scripts/check-msrv.sh、原生 workspace 全目标 Clippy、WASI CLI/config/driver 全目标 Clippy、fmt 和 wasi-only facade gate 通过。最终恢复 1.100 的原生 CLI/示例、Web/Component、WASI CLI 与全部九个 WASI 示例。

两编译器各自生成的 guest 均实际通过同一完整运行集合：52 个新恢复场景、25 个既有通知、四个低层游标、28 个队列、48 个凭据、50 个控制、52 个 Reactor、45 个 SDK HTTP/信任拒绝、60 个配置 CA 的 HTTPS 场景，以及 CLI 的 58 个 guest 与 WASI 配置矩阵 57 个进程；SDK 两种传输集合各启动 42 个官方 SDK server。该证据是本机 Linux/Wasmtime 50.0.1 和配置 CA embedding host，不能扩展为其他 OS、运行时或原版 CLI 公共根 HTTPS 的结论。52 个新场景在两编译器各执行一次仍只计为 52 个场景，不合并进 Rust 单测数量。

日志 target/mcpx-wasi-recovery-rust.log、msrv.log 为 Rust/floor gate；msrv-runtime.log、default-runtime.log 为两编译器完整真实 I/O 集合；clippy.log、native-clippy.log、build-restore.log、facade.log 为静态检查和最终构建，均使用 mcpx-wasi-recovery- 前缀。targeted-final.log 另保留默认编译器 52 项专项；debug-exchange-2.log 保留修复前可复现的空 GET flush Closed 证据，产品源码已移除所有调试输出。该问题的直接证据不等于更早历史单次 WASI 通知等待超时已确定根因。

本批未修改 Core/协议/WIT、JS 驱动或依赖锁文件，没有重复运行未受影响的完整浏览器/Worker 矩阵；其证据沿用上一批。CI 新增脚本仅已配置，没有执行远端 CI。通知恢复现已接入原生、JS/Worker 与 WASI 高层；业务响应游标、会话续建、其余完整资源预算、性能/发布和完整方案验收继续推进。


业务响应流恢复基础批次新增 14 项 Rust 测试（13 Core 集成、一项 Machine ID），Rust 1.88.0/1.100.0 各完整通过 360 项（359 单测/集成 + 一项 WIT doctest）。workspace 原生全目标 Clippy、WASI CLI/config/driver 全目标 Clippy、fmt、完整 scripts/check-msrv.sh 和 wasi-only facade 通过；最终恢复 1.100 的原生 CLI/示例、Web/Component、WASI CLI 与全部十个 WASI 示例。Core/响应 ABI、WIT、固定 adapter、测试/SDK fixture 与文档有改动；依赖锁文件未改。高层调用 driver 没有启用新开关，不能把本批当作完成高层自动业务恢复。

两个编译器生成的 Web/Component 分别完成完整 npm run test:wasm，包含 12 个新响应流 HTTP 场景、四个 SDK 场景（两项显式旧版在途恢复、两项明确版本拒绝）、原有通知/恢复、16 路调用、交互、Worker/CLI 和 retained Promise GC。两编译器的 WASI 各通过 11 套实际 I/O 集合：四个新响应流 guest、25 通知、52 通知恢复、四个低层通知游标、28 queue、48 credentials、50 controls、52 reactor、45 SDK HTTP/信任拒绝，以及 CLI 58 guest 和配置矩阵 57 进程。按场景计数，不将两编译器重复执行或 guest 数合并为 Rust 单测。配置 CA 的 HTTPS embedding 矩阵未在这次无 TLS 修改的批次重复运行，沿用上一批实际证据；不扩展为更多 OS、运行时或远端 CI 已通过。

日志统一前缀 target/mcpx-response-resume-：rust/msrv 为完整 Rust/floor gate，msrv/default-web 为两个编译器 Web/Worker/GC gate，msrv/default-wasi 为各 11 套 guest/CLI 集合，clippy/wasi-clippy、restore、facade 为静态检查和最终构建。core-all、http、wasi-http、sdk 为此前专项证据。sdk-debug 与早期 sdk 日志保留官方 SDK 默认 request-wise 完成路由已删除、3000ms retry 后空 GET 的真实失败；最终 SDK 场景明确限制为在途恢复，SDK session retry 配置与前端切流方式写在上节，未改 SDK 源码、事件 ID 或 JSON-RPC 响应。本地测试不是完整协议 conformance 或远端 CI 验收。

新增 CI 将响应恢复纳入 Web gate，并在 msrv/wasi-cli 各加入四个 WASI guest；Web job 先构建其本地官方 SDK daemon。后续继续接入高层 Native/JS/Worker/WASI 业务响应恢复策略/调度、旧版会话续建与其余完整计划，持续目标保持不变。

原生业务响应恢复批次新增 17 项 Rust 测试（16 个真实 HTTP 生命周期/并发测试，一个官方 SDK 测试包含 mutable/queued 两种客户端），Rust 1.88.0/1.100.0 各完整通过 377 项（376 单测/集成 + 一项 WIT doctest）。workspace 全目标 Clippy（-D warnings）、fmt 和完整 scripts/check-msrv.sh 通过；最终恢复默认 1.100 的原生 CLI/全部原生示例、Web/Component、WASI CLI/全部十个 WASI 示例，并检查无 native 的 facade 组合。日志前缀 target/mcpx-native-response-：rust/msrv 为完整回归及最低版本 gate，clippy/fmt 为静态检查，targeted/sdk 为专项，restore 为最终构建。检查中修复了关闭队列时遗漏已接受结果但尚待回复清理的操作：关闭现回收对应任务并返回已知结果，独立测试保留成功及 provider 释放证据。

本批修改 Native builder/driver/并行调度、共享原生 HTTP reader、测试与文档；仅增加测试用 Tokio rt-multi-thread feature，依赖版本与锁文件未改。Core/协议、Wasm/WIT、JS/Worker 和 WASI driver 无变更，未重复运行上一批已通过的完整浏览器/Worker/guest 运行矩阵；本批跨目标证据为两编译器构建 gate，不能替代运行验证。既有 workspace CI 会收录新 Rust 测试，未执行远端 CI。JS/Worker/WASI 高层业务恢复、session 续建、完整 Schema 工作/内存预算及其余性能/发布/平台验收继续按完整技术方案推进。

JS/Worker 业务响应恢复批次新增一项 Core 集成测试与固定 Wasm/WIT 耗尽报告方法，两编译器各完整通过 378 项 Rust 测试（377 单测/集成 + 一项 WIT doctest）。Rust 1.100 的完整 npm run test:wasm 通过，包含旧 Web/Worker/目录/凭据/通知/恢复/lifecycle 回归、新增 200 场景 + 一项事件 ACK 专项 + 八组 GC，以及扩展后的八个官方 SDK 场景。Rust 1.88 在完整 scripts/check-msrv.sh 后实际构建 Wasm/Component，并运行 smoke-wasm、typed Component、12 个低层响应恢复、八个 SDK、新高层恢复/ACK/GC 矩阵，另运行 Wasmtime 四个低层响应恢复 guest。每套新高层矩阵均观察 53 个 Worker 退出、HTTP 流关闭及相关凭据回收。

证据前缀 target/mcpx-js-response-：rust/msrv 为完整 Rust/floor gate，web 为完整默认工具链 Web gate，msrv-build/msrv-runtime 为最低版本构建和上述实际运行，targeted/sdk/core 为早期专项，clippy/wasi-clippy/fmt 为静态检查，restore 为默认工具链最终原生 CLI/示例、Wasm/Component（含 adapter）、WASI CLI/全部十个示例与 facade 恢复构建，并再次运行默认工具链的四个 WASI 低层恢复 guest。workspace 与 WASIp2 全目标 Clippy（-D warnings）、fmt 均通过。依赖版本/锁文件未改，WIT 增加方法要求组件与固定 adapter 一起重建，尚未承诺 ABI 发布稳定性。

本批没有修改 WASI driver，也未重复全部十一套 WASI guest/CLI 集合及配置 CA 的 HTTPS 运行矩阵；相应历史证据仍按原范围保留，四个低层响应 guest 不能替代其验收。CI test:wasm 已自动收录新 JS 专项，没有执行远端 CI。Native 与 JS/Worker 高层业务响应恢复现已接入；后续继续 WASI 高层调度/策略、旧版 session 续建、完整 Schema CPU/内存预算和其余完整技术方案范围，持续目标保持不变。

WASI 高层业务响应恢复批次新增两项 Rust 策略边界单测，两编译器各完整通过 380 项（379 单测/集成 + 一项 WIT doctest）；完整 scripts/check-msrv.sh、workspace 全目标与 WASIp2 driver/CLI/config 全目标 Clippy（-D warnings）、fmt 均通过。两编译器各实际运行十三套 WASI 集合：新增 70 guest 高层响应专项、四个官方 SDK 恢复/版本拒绝 guest，以及原有通知、通知恢复、低层 listener/response 恢复、队列、凭据、控制、Reactor、SDK、CLI、命名配置十一套回归。共享 SDK 脚本的八个 Node Wasm/jco 原场景另通过；共享 SDK 脚本仅在非 WASI 分支导入 Web 构建产物，CI 的独立 WASI job 无需生成浏览器 bindings。

日志前缀 target/mcpx-wasi-response-：rust/msrv 为完整 Rust/floor gate，default-runtime/msrv-runtime 为两编译器十三套实际运行，targeted/sdk/js-sdk 为专项，check/build/all-build/cli-build 为较早构建，native-clippy/clippy/fmt 为静态检查，restore 为最终默认 1.100 原生 CLI/示例、Web/Component（含 adapter）、WASI CLI/全部十一个 WASI 示例及 facade 组合的恢复构建。新公共策略与共享 WASI I/O 调度、WASI 示例、SDK/专项脚本及文档有改动，Core/协议/WIT、JS 产品驱动、依赖版本和锁文件未改。既有 CI 的 msrv/wasi-cli 已收录两项新运行，没有执行远端 CI。

本批未重复未改动的完整浏览器/Worker gate，也未重复配置 CA 的完整 HTTPS embedding 矩阵；相应既有证据仍按原范围保留，新增恢复专项使用真实本地 HTTP 与 stock Wasmtime。各平台高层业务响应恢复现已接入，但这不等于 session 自动续建或业务重试，也不替代完整协议 conformance。会话续建、完整 Schema CPU/内存硬预算、性能报告、更多运行时/平台与发布接口等继续按原完整技术方案推进。

JS/Worker 显式 HTTP 重连批次沿用并实际重新通过两编译器各 380 项 Rust 测试（379 单测/集成 + 一项 WIT doctest），本批未新增 Rust 测试计数；已有 Core reset 的活跃/结果保护用例继续通过，新方法语义另在真实 Wasm/WIT ABI 验证。Rust 1.100 完整 npm run test:wasm 通过，包含 200 个重连场景、16 组 GC、132 epoch 压力及所有既有通知/响应恢复/Worker/CLI/lifecycle 集合。随后新增的迟到交互结果用例在最终完整重连专项通过，最终专项观察 56 个 Worker 退出。

Rust 1.88 完整 scripts/check-msrv.sh 通过后，实际构建 Wasm/Component，并运行 smoke-wasm、typed Component、完整 HTTP 重连/GC/132 epoch、201 个高层响应恢复/ACK 场景及八个官方 SDK 场景；迟到结果用例及 132 epoch 另在其构建产物上专项通过（两个 Worker 均退出）。workspace 与 WASIp2 CLI/config/driver 全目标 Clippy（-D warnings）、fmt、facade 组合均通过，最终恢复默认 1.100 的原生 CLI/示例、Web/Component（含 adapter）、WASI CLI/全部十一个示例。

日志前缀 target/mcpx-http-reconnect-：rust/msrv 为两编译器完整 Rust/floor gate，web 为默认完整 Web gate，targeted 为首轮重连专项，final-targeted 为包含最终迟到用例的完整专项，msrv-build/msrv-runtime 为最低版本构建及运行，msrv-edges 为其迟到/132 epoch 专项，build/restore 为默认构建与最终恢复，clippy/wasi-clippy/fmt 为静态检查。本批修改 Wasm/WIT 导出、固定 adapter、JS/Worker owner/消息协议、脚本和文档；Core、Native/WASI driver、依赖版本/锁文件未改。WIT 增加方法要求组件与 adapter 一起重建。CI test:wasm 自动收录新脚本，没有执行远端 CI。

未重复未改动的十三套独立 WASI guest/CLI 与配置 CA 的 HTTPS embedding 集合，历史证据按原范围保留。新增 reconnect 是显式元数据操作，不能当成已完成 HTTP 404 自动续建、通用失败重试或 WASI 高层重连。后续继续这些生命周期接入、完整 Schema/allocator 工作预算、性能报告与其余完整技术方案验收，持续目标不缩小。


WASI 显式 HTTP 重连批次继续实际通过 Rust 1.100/1.88 各 380 项 Rust 测试（379 单测/集成 + 一项 WIT doctest），未把新增的真实 Wasmtime guest 算为 Rust 单测。两个编译器的最终产物各通过十四套实际 WASI I/O 集合，包含最终 138 个新重连场景、70 个业务响应恢复、四个 SDK 业务响应版本场景、25 个通知、52 个通知恢复、四个低层通知游标、四个低层业务游标、28 queue、48 credentials、50 controls、52 reactor、51 SDK/信任拒绝 guest、58 CLI guest 和配置 57 进程。SDK 集合现含六个新增 reconnect 场景（JSON/SSE × 现代/旧版/Auto），实际使用 48 个官方 SDK server。

配置 CA 的独立 HTTPS embedding 矩阵在两编译器 guest 上各完整通过 66 项（48 个 SDK server 场景含新增 reconnect，另有 18 个信任/origin/redirect 拒绝）。默认 workspace 与 WASIp2 CLI/config/driver 的全目标 Clippy（-D warnings）、fmt、facade 组合通过；最终恢复默认 1.100 的原生 CLI/示例、Web/Component（含固定 adapter）及 WASI CLI/全部十二个示例，并实际通过恢复产物的 Web/typed Component smoke。没有执行远端 CI 或扩大为其他 runtime/OS 已验证。

最低版本第一轮运行暴露 legacy_resume 独立低层测试辅助程序的空 GET body 竞态：它在对端可能已响应后才 blocking_write_and_flush 空流，偶发 StreamError::Closed。该辅助程序现与既有产品 Exchange 和 response_resume helper 一样，在 dispatch 前结束空 body；未加入 HTTP 重试或忽略错误。修复后的最低版本额外连续十轮、40 guest 通过，再分别完成两编译器的最终十四套矩阵。

证据日志前缀 target/mcpx-wasi-reconnect-：rust/msrv 是 Rust 与 floor gate；default-final-runtime/msrv-final-runtime 及编号子日志为最终十四套实际集合；final-targeted 是默认最终 138 个重连专项；default-https/msrv-https 为各 66 项 HTTPS；msrv-fixture-stress 为十轮空 GET helper 验证；restore/web-restore 为默认产物恢复与 ABI smoke；clippy/wasi-clippy/fmt 为静态检查。default-runtime 是修复辅助程序前的第一轮全部通过记录（其重连集最初为 130 项），msrv-runtime 保留发现竞态时的失败记录；后续成功证据不覆盖这些原记录。

本批变更 WASI driver/Queue/Reactor、真实 guest/脚本、CI 收录和文档；Core、Native、JS、Wasm/WIT ABI、依赖版本与锁文件未变。显式重连已覆盖各高层平台，HTTP 404/session 自动续建、完整 Schema CPU/allocator 硬预算、性能报告、更多 SDK/宿主/平台及发布接口等仍按完整技术方案继续，持续目标尚未完成。


原生 HTTP session 续建批次新增 24 项 Rust 测试：四项共享 Core、19 项原生真实 HTTP 场景及一项 stock rmcp SDK 测试（该测试实际运行 JSON/SSE 两种响应偏好）。Rust 1.100 和 1.88 最终各完整通过 404 项测试（403 单测/集成 + 一项 WIT doctest）。SDK 使用公开 LocalSessionManager.close_session 真实终止会话，失败请求收到 SDK 的 404，新的 session map 与实际工具 handler 计数均被检查；没有用代理注入失效或修改 SDK 实现。

因为共享 Core 的普通 POST 404 分类与请求 session 快照发生变化，本批实际重建两编译器的 Web/Component 与 WASI 交付，并分别完整通过 npm run test:wasm 与十四套 WASI I/O 集合，覆盖三类 HTTP body/游标、138 个显式重连 guest、官方 SDK 与 CLI 等已有路径。自动 session 续建本身仅由原生已连接队列 opt-in；不能将这些跨平台回归当成 JS/Worker/WASI 已接入同一自动策略。配置 CA 的独立 HTTPS embedding 集合本批未重复，沿用上一批各 66 项实际证据，未扩张为新运行时或远端 CI 已通过。

workspace 与 WASIp2 CLI/config/driver 的全目标 Clippy（-D warnings）、fmt、facade 组合通过；最终恢复默认 1.100 的原生 CLI/示例、Web/Component 与固定 adapter、WASI CLI/十二个示例，并实际运行恢复后的 Web/typed Component smoke。CI 的现有 cargo test --workspace 自动收录新增 Rust/SDK 测试；未执行远端 CI。

证据前缀 target/mcpx-http-session-：final-native/msrv-final-rust 为最终各 404 项测试（前者还包含最终默认 Clippy/fmt/原生产物恢复），official-sdk 为 stock SDK 专项，final-targeted 为 SDK 专项之前的原生恢复验证记录；web/msrv-web 为两编译器完整 Web gate，default-wasi/msrv-wasi 及编号子日志为各十四套实际集合。msrv 为新增最后几项场景前的完整 floor/build gate，msrv-wasm-build 为最低版本交付生成；build/restore/restored-smoke、clippy/wasi-clippy/fmt 为默认构建、恢复与静态检查。初始 rust/check/targeted 日志仅用于开发过程，最终计数以上述 final 记录为准。

本批未改变依赖/锁文件、Wasm/WIT record shape 或工具专属代码生成策略。后续继续初次连接、mutable/JS/Worker/WASI 的自动 session 生命周期接入、完整 Schema CPU/allocator 硬预算、性能报告、更多 SDK/宿主/平台、发布接口等原完整方案内容。持续目标仍未完成。


原生初始 session 续建批次新增十三项 Rust 测试（十二项真实 HTTP 测试及一项 stock SDK 测试），两编译器最终各完整通过 417 项（416 单测/集成 + 一项 WIT doctest）。会话测试文件现有 31 项场景测试，其中启动成功用例实际覆盖 mutable/Queue × 显式/Auto × ACK/首 tools/list 八种组合；SDK 测试文件有两项，新增启动用例实际使用四个 SDK server 配置。测试先暴露启动第一轮 finish 后旧取消句柄脱离后续轮次的问题，再验证稳定的原始启动令牌可以中断 provider 和 30 秒 backoff；失败的开发日志不作为最终通过依据。


两编译器分别构建原生 CLI/全部原生示例，并实际通过 smoke-native、smoke-library 与 native 配置 CLI（55 进程）集合；包含既有 HTTP/stdio、调用/checked-call、事件、队列与关闭路径。完整 check-msrv floor/build gate、默认 workspace 与 WASIp2 CLI/config/driver 全目标 Clippy（-D warnings）、fmt 和 facade 组合通过。本批没有改变 Core、协议/Wasm/WIT/JS、WASI 驱动、依赖或锁文件，没有重复这些未改动平台的完整运行矩阵与独立 HTTPS embedding 集合，相关证据按上一批真实范围保留。

最终恢复默认 1.100 的原生 CLI/全部原生示例、Web/Component 及固定 adapter、WASI CLI/十二个示例。证据前缀 target/mcpx-http-startup-：rust/msrv 为两编译器各 417 项与 floor gate，final-targeted 为最终 31 项会话测试与两项官方 SDK 测试（targeted 是开发阶段的对应记录），default-native/msrv-native 为各原生 CLI/库/配置实际运行，restore 为默认全平台构建恢复、Clippy/facade/fmt，restored-smoke 为恢复交付的 Web/typed Component smoke。源码的 SDK 测试只关闭真实 manager 中的 session，原始请求仍交由 stock SDK 决定 HTTP status。

原生初始 HTTP metadata 续建现在已接入；连接后的 mutable Remote、JS/Worker/WASI 的自动续建仍继续推进。原完整目标中的 Schema CPU/allocator 硬预算、更多运行时/平台与 SDK、性能报告、发布接口及最终逐项验收仍未完成，未缩小持续目标。


原生已连接 mutable Remote 续建批次新增十八项 Rust 测试（十五项真实 HTTP 场景、两项共享驱动及一项 stock SDK），Rust 1.100/1.88 最终各通过 435 项（434 单测/集成 + 一项 WIT doctest）。会话场景文件现有 46 项测试，SDK 文件三项；新增 SDK 用例在 JSON/SSE 两种响应偏好下真实关闭已有 manager 会话，再验证下一次操作恢复、旧绑定失效、原失败不重放和最终 manager 清空。新低层测试先复现未取走串行结果导致重建被 Busy 拒绝、owner 却被提前挂起的问题，再验证同步 reset 成功后才置 suspended 的修复；在途并发和保留结果的旧证据也继续通过。

两编译器交付分别实际通过完整 npm run test:wasm 和十四套 WASI runtime 集合，包括 Node Wasm/jco、Chromium、Worker、GC、官方 SDK、138 个显式重连与 70 个业务恢复 guest、WASI CLI 和 57 个配置进程。默认版本的完整 Web/WASI gate 在最后一项 native-only 准入保护调整前完成；该调整只在选择恢复钩子的 native owner 中生效，最低版本完整集合使用最终源码。两编译器原生 CLI/库/配置集合也分别通过，包含配置 CLI 的 55 个进程；最终原生源码另由两编译器各 435 项测试验证。独立配置 CA 的 HTTPS embedding 集合没有重复，沿用之前各 66 项证据，不能外推为更多平台、运行时或远端 CI。

证据前缀 target/mcpx-http-mutable-：rust/msrv 为最终两编译器各 435 项测试，msrv 同时包含完整 floor/build/facade gate；default-web/msrv-web、default-wasi/msrv-wasi 及编号子日志为各自实际运行集合；default-native/msrv-native 为原生 CLI/库/配置；serial-before 保存新边界修复前的可复现失败，不作通过依据。该批提取 Core drive_host、增加默认关闭的驱动恢复钩子与结果接收标记，并接入原生 mutable owner；Wasm/WIT record shape、JS/WASI 的驱动策略、依赖与锁文件没有改变，也没有生成工具专属 SDK。

默认 1.100 的原生 CLI/全部原生示例、Web/Component 和固定 adapter、WASI CLI/十二个示例已恢复构建；workspace 与 WASIp2 CLI/config/driver 全目标 Clippy（-D warnings）、facade 组合及 fmt 均通过，见 restore 日志。 恢复后另外实际通过 Web/typed Component 与原生库/队列示例 smoke，见 restored-smoke 日志。

原生初始连接、已连接 mutable 与 Queue 的 opt-in 有界 session 续建均已接入。JS/Worker/WASI 的自动 session 生命周期、完整 Schema CPU/allocator 硬预算、更多 SDK/宿主/平台、性能报告、公开接口和发布验收等原完整范围继续推进，持续目标仍未完成。
