## 📌 Project Overview

This project demonstrates an end-to-end **modern data platform** implementation for an **e-commerce analytics platform**, covering data ingestion from OLTP systems to analytics, business intelligence, and AI-powered SQL querying.

The pipeline extracts transactional data from two OLTP databases (**MySQL** and **Microsoft SQL Server**), loads it into **BigQuery** as a Bronze layer, transforms it using **dbt** into Silver and Gold layers, and serves analytics through **Power BI**.  
Additionally, an **AI-powered SQL Agent** was built using **Python, LLaMA (via Ollama), and LangChain**, allowing natural language querying directly against the Gold layer.

---

## 🏗️ Architecture

**High-level flow:**


---

## 🗂️ Data Sources (E-Commerce Domain)

The datasets represent a typical e-commerce business and include:

- **stores** – Store metadata and locations  
- **store_sales** – Daily/periodic sales per store  
- **products** – Product catalog and attributes  
- **customers** – Customer profiles and demographics  
- **orders** – Customer orders  
- **order_items** – Line-level order details  

---

## 🔄 Data Pipeline Details

### 1. OLTP Systems
- **MySQL**: Source system for part of the transactional data  
- **Microsoft SQL Server**: Secondary OLTP source  

Both systems simulate real-world production databases.

---

### 2. Python EL (Extract & Load)
- Extracts data from MySQL and MS SQL Server
- Performs minimal transformations (added a metadata/new columns "_extracted_at, _source_id, _batch_id) for incremental load.
- Loads raw data into **BigQuery Bronze layer**

---

### 3. BigQuery – Bronze Layer
- Stores **raw, untransformed** data
- Acts as the single source of truth for ingestion
- Preserves original schemas and data fidelity

---

### 4. dbt – Silver & Gold Layers

#### 🥈 Silver Layer
- Data cleaning and standardization
- Type casting and deduplication
- Referential integrity between tables
- Business-ready but still granular

#### 🥇 Gold Layer
- Analytics-ready fact and dimension tables
- Star-schema modeling
- Optimized for BI and AI querying

---

## 📊 Business Intelligence (Power BI)

- Power BI is connected **directly to the Gold layer**
- Dashboards include:
  - Sales performance
  - Store-level metrics
  - Product performance
- Designed for business users and stakeholders

---

## 🤖 AI SQL Agent

An AI-powered SQL assistant was built to enable **natural language analytics**.

### Tech Stack:
- **Python**
- **LLaMA** via **Ollama**
- **LangChain**
- **BigQuery (Gold Layer)**

### Features:
- Converts natural language questions into SQL
- Executes queries against the Gold layer
- Returns results in a readable format
- Enables non-technical users to explore data without writing SQL

---

## 🛠️ Tech Stack Summary

| Layer | Tools |
|-----|------|
| Databases | MySQL, Microsoft SQL Server |
| EL | Python |
| Data Warehouse | BigQuery |
| Transformations | dbt |
| BI | Power BI |
| AI / LLM | LLaMA (Ollama), LangChain |
| Programming | Python, SQL |

---

## 🎯 Key Learnings & Outcomes

- Designed a **scalable EL pipeline** from OLTP sources
- Implemented **medallion architecture** (Bronze / Silver / Gold)
- Built production-style **dbt models**
- Delivered **business-ready dashboards**
- Integrated **LLM-based analytics** with a modern data warehouse

---

## 🚀 Future Improvements

- Add orchestration (Airflow / Prefect)
- Implement data quality checks with dbt tests
- Row-level security in Power BI


---

## 📎 Author

**Bradley Alojado**  
BI Developer and soon to be Data Engineer / Analytics Engineer  
Portfolio Project #2

---

