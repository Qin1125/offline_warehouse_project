# 广电用户画像系统建设

> 基于 Hive + Spark 的离线用户画像系统，面向广电业务数据，产出 8 维度用户标签，支撑精细化运营。

## 📊 数据规模
- 5 张业务表（用户信息、账单、订单、状态变更、收视行为）
- 百万级用户数据

## 🛠️ 技术栈
- Hadoop / Hive（数据清洗与存储）
- Spark SQL / MLlib（特征工程 + SVM 建模）
- Python（脚本开发）

## 📁 项目结构
```text
├── docs/                     # 完整实验报告（PDF）
├── sql/ods/dml/              # Hive 清洗脚本
├── scripts/                  # Spark 特征工程与建模脚本
└── .gitignore