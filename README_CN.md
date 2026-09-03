![favicon](https://user-images.githubusercontent.com/547147/111869334-eb48f100-89a4-11eb-9c0c-bc74cdee197a.png)

# tg-archive (Telegram 静态归档工具)

[English](README.md) | [中文说明](README_CN.md)

**tg-archive** 是一个将 Telegram 群组或频道消息导出并归档为纯静态网站的工具，让群组聊天与频道历史能够像邮件列表存档一样便于查阅、搜索和长期保留。

## 效果预览
[@fossunited](https://tg.fossunited.org) 的 Telegram 群组归档效果：

![image](https://user-images.githubusercontent.com/547147/111869398-44188980-89a5-11eb-936f-01d98276ba6a.png)

## 工作原理
tg-archive 使用 [Telethon](https://github.com/LonamiWebs/Telethon) Telegram API 客户端将群组或频道中的消息增量同步到本地 SQLite 数据库中。随后通过 Jinja 模板引擎编译生成纯静态 HTML 页面与 RSS/Atom Feed，可直接部署在 GitHub Pages、Cloudflare Pages、Vercel、Nginx 或任何静态托管平台。

## 核心特性
- **富文本格式完整保留**：通过精确的 UTF-16 字节切片对齐 Telegram MTProto 实体，完整保留粗体、斜体、下划线、删除线、行内代码、多语言语法代码块、引用块、剧透遮盖（Spoiler）以及内嵌超链接，彻底告别字符漂移与标签乱码。
- **即时预览（Instant View）代理链接自动解包**：自动将受保护（禁止复制/转发）频道中的 `https://t.me/iv?rhash=...&url=...` 代理链接还原为其真实的原始 Telegraph 或网页地址。
- **极速增量静态构建**：重新执行构建时不再暴力清空并重新拷贝整个媒体目录，已有媒体文件增量保留，大幅缩短大型频道的构建耗时。
- **SQLite 深度优化**：建立专用日期、用户与回复关系索引，消除全表扫描瓶颈；全量遵循 Python 3.12+ ISO 8601 时间戳标准，零废弃警告。
- **现代化 Python 标准库适配**：拥抱 Python 3.9+ 内置 `zoneinfo` 与 `mimetypes`，彻底移除旧版 `pytz`、`python-magic`、`six`、`python-dateutil` 等冗余第三方依赖，在 Windows / Linux / macOS 上开箱即用（免去 Windows 缺失 libmagic C 动态库的报错困扰）。
- **媒体与投票本地归档**：本地下载用户头像、图片、文件、音频，完整渲染群组投票（Poll）统计结果。
- **多级时间线索引**：按年、月、日自动生成深链接时间线导航。
- **消息父子回复定位**：跨页面、跨月份自动追踪并链接回所回复的父级消息。
- **订阅源生成**：自动生成最近消息的 RSS 与 Atom Feed。
- **PEP 621 打包规范**：通过现代标准 `pyproject.toml` 管理依赖，全面兼容 Python 3.9 至 Python 3.14+。

## 环境要求与安装

### 前置条件
- Python 3.9+（已实测支持 Python 3.10、3.11、3.12、3.13、3.14）
- Telegram API 凭据：前往 [my.telegram.org](https://my.telegram.org/auth?to=apps) 获取（属于正常用户账号 API，非 Bot API）。

### 安装
```bash
git clone https://github.com/mhdy2233/tg-archive.git
cd tg-archive

# 安装本包及依赖
pip install .
```

## 使用指南

### 1. 初始化站点目录
```bash
tg-archive --new --path=mysite
cd mysite
```
或者使用模块命令：
```bash
python -m tgarchive --new --path=mysite
cd mysite
```
修改生成的 `config.yaml`，填入你的 `api_id`、`api_hash` 以及目标 `group`（频道的 `@username`、邀请链接标识或 `-100...` 形式的频道 ID）。

### 2. 同步 Telegram 消息
```bash
tg-archive --sync
```
- 首次连接时，终端会提示输入接收登录验证码的手机号（需带国际区号，如 `+86...` / `+852...`），并在 Telegram 官方号中接收验证码。若开启了两步验证（2FA），会进一步提示输入 2FA 密码。
- 登录成功后，会在本地生成 `session.session` 会话文件（**严禁公开分享此文件**，其包含你的账号登录授权）。
- 后续同步会自动读取已有数据库的最新一条消息，只拉取增量新消息。
- **指定起始消息 ID 同步**（例如只归档最近的消息）：
  ```bash
  tg-archive --sync --from-id 3876
  ```

### 3. 生成静态网站
```bash
tg-archive --build
```
执行后将在 `site/` 目录下生成完整的纯静态站点（包含 HTML 页面、分类索引、媒体附件及 `index.xml` RSS / `index.atom` Feed）。

## 自定义配置
- **模板修改**：修改站点根目录下的 `template.html` 来自定义页面结构与 Jinja2 排版。
- **样式定制**：修改 `static/style.css` 来自定义样式（已内置代码块、引用块及剧透点击显示样式）。
- **时区配置**：在 `config.yaml` 中配置 `timezone`（如 `Asia/Shanghai` 或 `Asia/Hong_Kong`），生成归档时将自动按本地时区呈现消息时间。

## 开源协议
本项目采用 MIT 许可证。
