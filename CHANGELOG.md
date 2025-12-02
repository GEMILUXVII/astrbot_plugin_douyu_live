# 更新日志

本文件记录 `astrbot_plugin_douyu_live` 插件的版本更新历史。

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
