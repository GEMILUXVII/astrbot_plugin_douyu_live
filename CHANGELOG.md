# 更新日志

本文件记录 `astrbot_plugin_douyu_live` 插件的版本更新历史。

## [1.4.0] - 2025-12-10

### 新增

- **订阅级别设置隔离**：`@全体成员`、`礼物播报`、`高价值过滤` 设置现在按群独立配置
  - 每个群可以对同一直播间设置不同的配置，互不影响
  - 新增 `SubscriptionConfig` 数据模型存储每个群的独立设置
- `/douyu mysub` 命令现显示当前群的订阅配置状态

### 变更

- `/douyu atall`、`/douyu gift`、`/douyu giftfilter` 命令现在只影响当前群的订阅
  - 执行这些命令前需要先在当前群订阅直播间
- `/douyu ls` 命令不再显示房间级别的设置（因设置现为订阅级别）
- 数据结构从 `dict[int, set[str]]` 改为 `dict[int, dict[str, SubscriptionConfig]]`

### 迁移

- 旧版数据格式会在首次加载时自动迁移
- 迁移时订阅配置会继承原房间级别的设置

---

## [1.3.0] - 2025-12-02

### 新增

- 新增 **下播通知**，自动计算本次直播时长并推送至订阅源
- 插件启动后若检测到直播已进行，立即推送一次开播通知
- 新增 GitHub Actions 工作流，依据 `v*` 标签自动打包并发布 Release

### 优化

- `DouyuMonitor` 新增状态冷却与一次性下播保护，解决开播即误判下播的问题
- `DouyuMonitor` 支持在冷却期内保持原状态，避免漏发真实下播通知
- `Notifier` 新增发送失败自动重试，并在重试时自动降级 @全体，降低风控概率

### 修复

- 修复 pydouyu 连接抖动导致的重复通知/漏通知问题
- 修复 NapCat 接口短暂失败时通知直接丢失的问题

---

## [1.2.2] - 2025-12-02

### 修复

- 修复数据加载时旧版本字段兼容性问题（`min_gift_price` -> `high_value_only`）
- 修复 pydouyu Socket 连接失败后无法恢复的问题
- 优化监控器连接逻辑，支持断线自动重连
- 修复插件重载时 Socket 状态异常的问题

### 变更

- `DouyuMonitor` 延迟创建 Client，避免初始化时的 Socket 问题
- 添加 `_stop_flag` 标志，实现优雅的线程停止
- 连接断开后 10 秒自动重连

---

## [1.2.1] - 2025-12-02

### 新增

- 新增 `/douyu giftfilter <房间号> [on/off]` 命令，可选择是否只播报高价值礼物
- 高价值礼物过滤默认开启，只播报飞机及以上的礼物

### 变更

- `RoomInfo` 数据模型将 `min_gift_price` 改为 `high_value_only` 布尔字段
- `/douyu ls` 命令现显示礼物过滤模式（仅高价值/全部）
- `/douyu gift` 命令现同时显示过滤模式状态

### 修复

- 修复礼物映射表中重复的键值
- 修复代码风格问题（ruff W293）

---

## [1.2.0] - 2025-12-02

### 新增

- **礼物播报功能**：直播间收到礼物时自动在订阅群内播报
  - 新增 `/douyu gift <房间号> [on/off]` 命令，管理员可开启/关闭礼物播报
  - 支持常见斗鱼礼物名称显示（粉丝荧光棒、飞机、火箭等）
  - 未知礼物显示为 "神秘礼物(ID)"
- 新增 `utils/constants.py` 礼物 ID 到名称的映射表

### 变更

- `RoomInfo` 数据模型新增 `gift_notify` 和 `min_gift_price` 字段
- `DouyuMonitor` 现支持 `live_callback` 和 `gift_callback` 双回调
- `Notifier` 新增 `build_gift_notification()` 方法
- `/douyu ls` 命令现显示礼物播报状态

---

## [1.1.0] - 2025-12-02

### 新增

- **项目模块化重构**：
  - `core/monitor.py` - 斗鱼监控器封装
  - `core/api.py` - 斗鱼 API 调用（自动获取主播名称）
  - `core/notifier.py` - 通知构建与发送
  - `storage/data_manager.py` - 数据持久化管理
  - `models/room.py` - 房间信息数据模型
  - `utils/constants.py` - 常量定义
- **自动获取主播名称**：添加房间时若未指定名称，自动从斗鱼 API 获取

### 依赖

- 新增 `httpx>=0.24.0` 用于异步 HTTP 请求

---

## [1.0.0] - 2025-11-28

### 初始版本

- 斗鱼直播开播通知功能
- 多房间监控支持
- 群订阅机制
- @全体成员通知选项
- 管理员权限控制

### 命令

- `/douyu add <房间号> [名称]` - 添加监控
- `/douyu del <房间号>` - 删除监控
- `/douyu ls` - 查看监控列表
- `/douyu sub <房间号>` - 订阅通知
- `/douyu unsub <房间号>` - 取消订阅
- `/douyu mysub` - 查看我的订阅
- `/douyu status` - 查看监控状态
- `/douyu restart [房间号]` - 重启监控
- `/douyu atall <房间号> [on/off]` - 设置@全体
