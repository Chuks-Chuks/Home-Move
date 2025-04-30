# 🏡 HomeMove Analytics Platform

A simulated end-to-end data engineering project that mirrors the operational needs of a business like Movera — built to demonstrate data pipeline, modeling, and reporting expertise.

## 📌 Business Goal

To improve operational visibility and customer satisfaction during the home moving process by consolidating data into a single analytics platform. This enables real-time tracking of transactions, bottlenecks, and customer sentiment.

## ⚙️ Project Architecture

| Layer             | Tools & Technologies                          |
|-------------------|-----------------------------------------------|
| Data Generation   | Python, Faker                                 |
| Ingestion         | Airflow / Python scripts                      |
| Storage           | Azure Blob / Local CSV                        |
| Transformation    | dbt (modular SQL models)                      |
| Orchestration     | Apache Airflow                                |
| Reporting         | Power BI or Streamlit                         |
| Monitoring        | Python (data freshness checks)                |
| CI/CD             | GitHub Actions                                |

## 🧱 Core Components

- **Simulated datasets** for customers, property transactions, CSAT surveys
- **Staging and mart models** using dbt for clean, analytics-ready tables
- **Dashboards** showing KPIs like transaction duration, CSAT scores, and drop-off rates
- **Monitoring tools** to detect stale data or ingestion failures


## 🚀 Getting Started

1. Clone the repo and run the data generator in `scripts/`
2. Set up your database and run the dbt models
3. Visualize with Power BI or build your own dashboard
4. Monitor your data pipelines and freshness

## 📊 Example KPIs Tracked

- Avg Time to Completion
- Drop-Off Rate
- CSAT Score (by team, region)
- Solicitor performance
- Pipeline health (latency, data freshness)

---

## 👨‍💻 Author

Philip Igbeka – [GitHub](https://github.com/Chuks-Chuks)

---


