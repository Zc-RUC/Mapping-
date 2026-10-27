**[English](README.en.md) | 中文**

# Mapping-

帮助 HR 完成自动化人才 Mapping 的爬虫工具集 —— 从目标实验室官网抓取学生信息，批量产出结构化名单，节省人工搜集时间。

## 覆盖来源

| 脚本 | 来源 |
|---|---|
| `scrape_madsys_students.py` | 清华大学 MADSys 实验室（madsys.cs.tsinghua.edu.cn） |
| `scrape_storage_students.py` | 清华大学存储研究组（storage.cs.tsinghua.edu.cn） |

## 产出字段

姓名、学生类型（博士/硕士）、实验室、个人简介、教育经历、获奖、教学经历、邮箱，以及自动检索补充的 Google Scholar / GitHub / LinkedIn / 个人主页链接（自动过滤搜索引擎与本校域名，并识别竞品高校标记）。导出 CSV / Excel（pandas）。

## 安装与使用

```bash
pip install pandas requests beautifulsoup4
python3 scrape_madsys_students.py -o madsys.csv
python3 scrape_storage_students.py -o storage.csv
```

脚本基于 argparse 提供多个可选参数（输出路径、限速、学校别名映射等），运行 `python3 scrape_madsys_students.py --help` 查看全部选项。

## 说明

- 仅抓取公开页面，自带请求间隔限速与标识 UA
- 请遵守目标站点 robots 协议与学校相关规定，数据仅限合法合规的招聘用途
