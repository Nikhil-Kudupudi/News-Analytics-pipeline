# News-Analytics-pipeline
---

## 🧩 **Problem Statement: Real-Time News Sentiment & Trend Analytics**

In today's hyper-connected world, organizations struggle to keep up with the **real-time flood of global news**. Whether it's a **brand crisis**, **market-moving headline**, or **emerging topic**, the ability to monitor and analyze news sentiment and trends in real time is critical for **decision-makers in finance, media, government, and marketing**.

This project aims to solve that problem by building a **real-time data engineering pipeline** that continuously ingests breaking news headlines from public APIs, processes them through a streaming NLP engine, and transforms them into actionable insights using modern data tooling.

The system leverages:

* **Kafka** for event streaming,
* **Apache Spark Structured Streaming** for real-time processing and sentiment analysis,
* **dbt** for transforming enriched data into analytical models (e.g., trending topics, sentiment timelines),
* **Airflow** for orchestrating the pipeline, and
* **Snowflake/Delta Lake** for scalable storage and querying.

The final insights are visualized through a lightweight **Streamlit dashboard** to track:

* Top keywords & named entities,
* Trending topics per category/source,
* Sentiment fluctuations across time,
* Alerts for spikes in negative news.

---
