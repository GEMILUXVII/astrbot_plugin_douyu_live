<div align="center">
  <img src="logo.png" alt="AstrBot Douyu Plugin Logo" width="160" />
</div>

# <div align="center">Douyu Live</div>

<div align="center">
  <strong>AstrBot 斗鱼直播间监控与通知插件</strong>
</div>

<br>

<div align="center">
  <a href="CHANGELOG.md"><img src="https://img.shields.io/badge/version-v2.2.0-9644F4?style=for-the-badge" alt="Version"></a>
  <a href="https://github.com/GEMILUXVII/astrbot_plugin_douyu_live/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-AGPL--3.0-E53935?style=for-the-badge" alt="License"></a>
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"></a>
  <a href="https://github.com/AstrBotDevs/AstrBot"><img src="https://img.shields.io/badge/AstrBot-Compatible-00BFA5?style=for-the-badge&logo=robot&logoColor=white" alt="AstrBot Compatible"></a>
</div>

<div align="center">
  <a href="https://www.douyu.com/"><img src="https://img.shields.io/badge/Douyu-Live-FF9800?style=for-the-badge&logo=livejournal&logoColor=white" alt="Douyu"></a>
  <a href="https://github.com/NapNeko/NapCatQQ"><img src="https://img.shields.io/badge/NapCat-QQ-2196F3?style=for-the-badge&logo=qq&logoColor=white" alt="NapCat"></a>
  <a href="https://github.com/GEMILUXVII/astrbot_plugin_douyu_live/commits/master"><img src="https://img.shields.io/badge/updated-2026--07--27-0097A7?style=for-the-badge&logo=calendar&logoColor=white" alt="Last Updated"></a>
</div>

<br>

<div align="center">
  <a href="#插件简介">插件简介</a> •
  <a href="#功能特性">功能特性</a> •
  <a href="#命令列表">命令列表</a> •
  <a href="CHANGELOG.md">更新日志</a>
</div>

## 插件简介

AstrBot 斗鱼直播通知插件，支持多房间监控、订阅推送、@全体成员、数据持久化等功能。适用于 QQ 群、私聊等多平台，助你不错过任何开播时刻！

## 功能特性

- **多房间监控**：同时监控多个斗鱼直播间，自动检测开播
- **订阅推送**：用户可自主订阅/取消订阅，精准推送到群/私聊
- **@全体成员**：支持开播时自动 @全体成员（可选）
- **下播通知**：自动推送下播提醒并附带当次直播时长
- **抗抖动机制**：内置状态冷却、重试与自动恢复，避免重复或漏报
- **断连补偿**：每次弹幕重连后自动用 HTTP 接口对账开播状态，补齐断连
  窗口内丢失的开播/下播（对账失败按 5/10/20/40/60s 退避重试直到成功）
- **可靠推送**：通知发送失败按 5/15/45/120s 指数退避重试，只补发失败目标
- **状态透明**：`/douyu ls` 展示每个房间的监控状态——🟢 运行中 /
  🟡 重连中（弹幕连接暂不可用，库在自动重连）/ 🔴 已停止
- **自动获取主播名**：添加房间时自动从斗鱼获取主播名称
- **数据持久化**：监控与订阅数据自动保存，重启不丢失
- **权限控制**：添加/删除直播间需管理员权限
- **状态查询**：随时查看监控与订阅状态

## 安装与配置

1. **安装插件**

   将本插件目录放入 AstrBot 的 `data/plugins/` 目录下：

   ```
   data/plugins/astrbot_plugin_douyu_live/
   ├── main.py
   ├── metadata.yaml
   ├── requirements.txt
   ├── core/
   ├── models/
   ├── storage/
   └── utils/
   ```

2. **重启/重载 AstrBot**

   在 WebUI 重载插件，或直接重启 AstrBot。AstrBot 会自动安装所需依赖（[`aiodouyu`](https://github.com/GEMILUXVII/aiodouyu)）。

## 命令列表

### 管理员命令

| 命令                                  | 说明                | 示例                             |
| ------------------------------------- | ------------------- | -------------------------------- |
| `/douyu add <房间号> [名称]`     | 添加监控直播间 | `/douyu add 12725169 某主播` |
| `/douyu del <房间号>`            | 删除监控直播间 | `/douyu del 12725169`        |
| `/douyu atall <房间号> [on/off]` | 设置 @全体成员 | `/douyu atall 12725169 on`   |
| `/douyu restart [房间号]`        | 重启监控       | `/douyu restart`             |

> `atall` 为**群级设置**：只影响当前群的订阅，需要先在当前群 `/douyu sub` 订阅后才能设置，不会影响订阅了同一直播间的其他群。

### 普通用户命令

| 命令                    | 说明           | 示例                    |
| ----------------------- | -------------- | ----------------------- |
| `/douyu help`           | 命令帮助       | `/douyu help`           |
| `/douyu ls`             | 查看监控列表   | `/douyu ls`             |
| `/douyu live`           | 查看当前在播   | `/douyu live`           |
| `/douyu sub <房间号>`   | 订阅直播间通知 | `/douyu sub 12725169`   |
| `/douyu unsub <房间号>` | 取消订阅       | `/douyu unsub 12725169` |
| `/douyu offline <房间号> [on/off]` | 本群下播通知开关 | `/douyu offline 12725169 off` |
| `/douyu mysub`          | 查看我的订阅   | `/douyu mysub`          |
| `/douyu status`         | 查看监控状态   | `/douyu status`         |

> 2.2.0 起支持 WebUI 插件配置(通知富化/封面图/重启补播报/通知冷却/
> 订阅权限档位/场次保留天数),见 AstrBot 管理面板的插件配置页。
> 开播通知默认附带直播间标题、分类与封面图;场次历史落盘于插件数据
> 目录 `sessions/`,供后续统计功能使用。

## 使用示例

### 添加直播间（管理员）

```
/douyu add 12725169
```

不提供名称时，插件会自动从斗鱼获取主播名称。

### 用户订阅

```
/douyu sub 12725169
```

### 开启 @全体成员

```
/douyu atall 12725169 on
```

### 查看监控状态

```
/douyu status
```

## 通知样例

### 开播通知

```
@全体成员
🎉 斗鱼直播开播通知
━━━━━━━━━━━━━━
👤 主播: 某主播
🔢 房间号: 12725169
⏰ 时间: 2024-01-01 20:00:00
🔗 链接: https://www.douyu.com/12725169
━━━━━━━━━━━━━━
快去观看吧！
```

### 下播通知

```
📴 斗鱼直播下播通知
━━━━━━━━━━━━━━
👤 主播: 某主播
🔢 房间号: 12725169
⏱️ 本次直播时长: 45分钟
⏰ 下播时间: 2025-12-02 21:02:53
━━━━━━━━━━━━━━
感谢观看，下次再见！
```

## 数据存储

插件数据默认存储于：

```
data/plugin_data/astrbot_plugin_douyu_live/douyu_live_data.json
```

写入为原子操作，并自动保留上一代备份（`douyu_live_data.json.bak`）。若主文件损坏，
插件会自动从备份恢复，并将损坏文件隔离为 `douyu_live_data.json.corrupt.<时间戳>` 以便手工恢复。

数据结构示例（1.4.0 起通知设置为**订阅级**配置，挂在每个群的订阅下）：

```json
{
  "subscriptions": {
    "12725169": {
      "default:GroupMessage:123456789": {
        "at_all": true,
        "subscribed_by": "订阅者ID"
      }
    }
  },
  "room_info": {
    "12725169": {
      "name": "主播名称",
      "added_by": "管理员ID",
      "added_time": "2024-01-01 12:00:00"
    }
  },
  "unsub_history": {}
}
```

> 数据文件中包含群/用户会话标识，建议避免将数据目录设置为对其他用户可读。

## 常见问题

### Q: 监控启动失败

 A: 检查房间号是否正确、网络是否可用，并查看 AstrBot 日志获取详细错误。

### Q: @全体成员 不生效

A: 请确保已开启 @全体成员，且机器人有群管理员权限，群设置允许 @全体成员。

### Q: 收不到开播通知

A: 用 `/douyu ls` 查看状态：🟡 重连中表示弹幕连接暂不可用（网络或斗鱼
侧问题，库会自动重连并在重连后对账补发）；🔴 已停止时 watchdog 会在
1 分钟内自动重启监控。持续异常再手动 `/douyu restart`。

### Q: 重复收到通知

A: 分两种情况：

- **`/douyu restart` 或 watchdog 重启监控**（进程内）：v2.0.0 起继承直播
  状态，不会对正在直播的房间重复播报；
- **AstrBot 宿主重启**：若重启时主播仍在直播，会**补发一次**开播通知——
  这是有意行为（1.3.0 起），且 2.1.0 迁移后由"偶发"变为必现（重连对账
  必然检测到在播状态）。后续版本将随场次历史落盘改为可配置。

主播端短暂闪断由冷却校准机制处理。上述之外的重复请附日志反馈 issue。

## 相关链接

- [AstrBot 官方文档](https://astrbot.app/)
- [AstrBot 插件开发指南](https://docs.astrbot.app/dev/star/plugin-new.html)
- [斗鱼直播](https://www.douyu.com/)
- [aiodouyu](https://github.com/GEMILUXVII/aiodouyu) — 本插件使用的斗鱼弹幕/房间信息 asyncio 库

## 许可证

[![](https://www.gnu.org/graphics/agplv3-155x51.png "AGPL v3 logo")](https://www.gnu.org/licenses/agpl-3.0.txt)

Copyright (C) 2025 GEMILUXVII

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
