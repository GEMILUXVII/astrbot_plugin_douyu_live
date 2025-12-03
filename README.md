<div align="center">
  <img src="LOGO.png" alt="AstrBot Douyu Plugin Logo" width="160" />
</div>

# <div align="center">AstrBot 斗鱼直播通知插件</div>

<div align="center">
  <a href="#插件简介">插件简介</a> •
  <a href="#功能特性">功能特性</a> •
  <a href="#命令列表">命令列表</a> •
  <a href="CHANGELOG.md">更新日志</a>
</div>

<div align="center">
  <a href="#更新日志"><img src="https://img.shields.io/badge/version-v1.4.0-9644F4?style=for-the-badge" alt="Version"></a>
  <a href="https://github.com/GEMILUXVII/astrbot_plugin_douyu_live/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-AGPL--3.0-E53935?style=for-the-badge" alt="License"></a>
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"></a>
  <a href="https://github.com/AstrBotDevs/AstrBot"><img src="https://img.shields.io/badge/AstrBot-Compatible-00BFA5?style=for-the-badge&logo=robot&logoColor=white" alt="AstrBot Compatible"></a>
</div>

<div align="center">
  <a href="https://www.douyu.com/"><img src="https://img.shields.io/badge/Douyu-Live-FF9800?style=for-the-badge&logo=livejournal&logoColor=white" alt="Douyu"></a>
  <a href="https://github.com/NapNeko/NapCatQQ"><img src="https://img.shields.io/badge/NapCat-QQ-2196F3?style=for-the-badge&logo=qq&logoColor=white" alt="NapCat"></a>
  <a href="https://github.com/GEMILUXVII/astrbot_plugin_douyu_live/commits/master"><img src="https://img.shields.io/badge/updated-2025--12--02-0097A7?style=for-the-badge&logo=calendar&logoColor=white" alt="Last Updated"></a>
</div>

<div align="center"><em>多房间监控，智能推送，斗鱼开播通知全自动！</em></div>

## 插件简介

AstrBot 斗鱼直播通知插件，支持多房间监控、订阅推送、@全体成员、礼物播报、数据持久化等功能。适用于 QQ 群、私聊等多平台，助你不错过任何开播时刻！

## 功能特性

- **多房间监控**：同时监控多个斗鱼直播间，自动检测开播
- **订阅推送**：用户可自主订阅/取消订阅，精准推送到群/私聊
- **@全体成员**：支持开播时自动 @全体成员（可选）
- **礼物播报**：支持直播间礼物实时播报，可过滤低价值礼物
- **下播通知**：自动推送下播提醒并附带当次直播时长
- **直播总结**：下播时自动生成礼物统计总结（收礼数量、金额、榜一大哥等）
- **抗抖动机制**：内置状态冷却、重试与自动恢复，避免重复或漏报
- **自动获取主播名**：添加房间时自动从斗鱼获取主播名称
- **数据持久化**：监控与订阅数据自动保存，重启不丢失（SQLite）
- **权限控制**：添加/删除直播间需管理员权限
- **状态查询**：随时查看监控与订阅状态

## 安装与配置

1. **安装依赖**

   ```bash
   pip install pydouyu httpx
   ```

2. **安装插件**

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

3. **重启/重载 AstrBot**

   在 WebUI 重载插件，或直接重启 AstrBot。

## 命令列表

### 管理员命令

| 命令                                  | 说明                | 示例                             |
| ------------------------------------- | ------------------- | -------------------------------- |
| `/douyu add <房间号> [名称]`          | 添加监控直播间      | `/douyu add 12725169 某主播`     |
| `/douyu del <房间号>`                 | 删除监控直播间      | `/douyu del 12725169`            |
| `/douyu atall <房间号> [on/off]`      | 设置 @全体成员      | `/douyu atall 12725169 on`       |
| `/douyu gift <房间号> [on/off]`       | 开启/关闭礼物播报   | `/douyu gift 12725169 on`        |
| `/douyu giftfilter <房间号> [on/off]` | 开启/关闭高价值过滤 | `/douyu giftfilter 12725169 off` |
| `/douyu summary <房间号> [on/off]`    | 开启/关闭直播总结   | `/douyu summary 12725169 on`     |
| `/douyu restart [房间号]`             | 重启监控            | `/douyu restart`                 |

### 普通用户命令

| 命令                    | 说明           | 示例                    |
| ----------------------- | -------------- | ----------------------- |
| `/douyu ls`             | 查看监控列表   | `/douyu ls`             |
| `/douyu sub <房间号>`   | 订阅直播间通知 | `/douyu sub 12725169`   |
| `/douyu unsub <房间号>` | 取消订阅       | `/douyu unsub 12725169` |
| `/douyu mysub`          | 查看我的订阅   | `/douyu mysub`          |
| `/douyu status`         | 查看监控状态   | `/douyu status`         |

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

### 开启礼物播报

```
/douyu gift 12725169 on
```

默认只播报高价值礼物（飞机及以上），如需播报所有礼物：

```
/douyu giftfilter 12725169 off
```

### 开启 @全体成员

```
/douyu atall 12725169 on
```

### 开启直播总结

```
/douyu summary 12725169 on
```

下播时会自动发送礼物统计总结。

### 查看监控状态

```
/douyu status
```

## 通知样例

### 开播通知

```
@全体成员
斗鱼直播开播通知
━━━━━━━━━━━━━━
主播: 某主播
房间号: 12725169
时间: 2024-01-01 20:00:00
链接: https://www.douyu.com/12725169
━━━━━━━━━━━━━━
快去观看吧！
```

### 礼物播报

```
斗鱼直播礼物播报
━━━━━━━━━━━━━━
直播间: 某主播
用户: 土豪用户
礼物: 火箭 x1
时间: 20:30:45
```

### 下播通知

```
斗鱼直播下播通知
━━━━━━━━━━━━━━
主播: 某主播
房间号: 12725169
本次直播时长: 45分钟
下播时间: 2025-12-02 21:02:53
━━━━━━━━━━━━━━
感谢观看，下次再见！
```

### 直播总结

```
📊 直播总结
━━━━━━━━━━━━━━
直播间: 某主播
房间号: 12725169
直播时长: 2小时30分
━━━━━━━━━━━━━━
🎁 礼物统计
收礼次数: 156
礼物总价值: ¥1,234.50
送礼人数: 42
━━━━━━━━━━━━━━
🏆 榜一大哥: 土豪用户
贡献: ¥500.00
━━━━━━━━━━━━━━
感谢大家的支持！
```

## 数据存储

插件数据存储于：

- **订阅配置**: `data/plugin_data/astrbot_plugin_douyu_live/douyu_live_data.json`
- **直播会话**: `data/plugin_data/astrbot_plugin_douyu_live/sessions.db`（SQLite）

JSON 数据结构示例：

```json
{
  "subscriptions": {
    "12725169": ["default:GroupMessage:123456789"]
  },
  "room_info": {
    "12725169": {
      "name": "主播名称",
      "added_by": "管理员ID",
      "added_time": "2024-01-01 12:00:00",
      "at_all": true,
      "gift_notify": true,
      "high_value_only": true,
      "summary_notify": true
    }
  }
}
```

SQLite 数据库包含以下表：

- **sessions**: 存储每场直播的基本信息（开始/结束时间、礼物统计等）
- **gifts**: 存储每场直播的详细礼物记录

## 常见问题

### Q: 提示 "pydouyu 库未安装"

A: 请运行 `pip install pydouyu` 安装依赖。

### Q: 监控启动失败

A: 检查房间号是否正确、网络是否可用，并查看 AstrBot 日志获取详细错误。

### Q: @全体成员 不生效

A: 请确保已开启 @全体成员，且机器人有群管理员权限，群设置允许 @全体成员。

### Q: 收不到开播通知

A: 检查监控状态、订阅状态，必要时重启监控。

### Q: 礼物播报太频繁

A: 使用 `/douyu giftfilter 房间号 on` 开启高价值过滤，只播报飞机及以上的礼物。

### Q: 重复收到通知

A: 可能因主播频繁开关播或插件重启导致，属正常偶发现象。

## 相关链接

- [AstrBot 官方文档](https://astrbot.app/)
- [AstrBot 插件开发指南](https://astrbot.app/dev/plugin.html)
- [斗鱼直播](https://www.douyu.com/)

## 许可证

[![](https://www.gnu.org/graphics/agplv3-155x51.png "AGPL v3 logo")](https://www.gnu.org/licenses/agpl-3.0.txt)

Copyright (C) 2025 GEMILUXVII

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
