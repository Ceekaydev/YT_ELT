# 📺 YouTube Data Engineering Pipeline — MrBeast Channel

[![Python](https://img.shields.io/badge/Python-3.10+-blue)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Airflow-2.x-brightgreen)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue)](https://www.docker.com/)
[![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-black)](https://github.com/features/actions)

A **production-style data engineering pipeline** that extracts YouTube data from the **MrBeast channel**, transforms it with business logic and AI-powered sentiment analysis, validates data quality, and deploys everything with **Airflow, Docker, Soda, and GitHub Actions CI/CD**.

---

## 📌 Table of Contents
- [Project Overview](#project-overview)
- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Pipeline Workflow](#pipeline-workflow)
- [Airflow DAG Flow](#airflow-dag-flow)
- [Project Structure](#project-structure)
- [Data Quality & Testing](#data-quality--testing)
- [CI/CD Workflow](#cicd-workflow)
- [Environment & Secrets](#environment--secrets)
- [How to Run](#how-to-run)

---

## 🧠 Project Overview

This project builds an **end-to-end ELT pipeline** that:

### ✅ Extracts
- Data from the **YouTube Data API**
- Targets the **MrBeast channel**
- Collects:
  - Video ID
  - Title
  - Duration
  - View count
  - Like count
  - Comment count
  - Publish date
- Stores raw data as **JSON (Bronze layer)**

### ✅ Loads
- Uses **PostgreSQL** as the data warehouse
- Creates **staging and core tables**

### ✅ Transforms
- Classifies videos into:
  - `short`
  - `normal`
- Adds **AI-powered sentiment analysis** on video titles using **HuggingFace**
- Converts YouTube `PT` duration format into proper timestamps

### ✅ Orchestrates
- Uses **Apache Airflow** with:
  - Scheduler
  - Webserver
  - Workers
  - Redis
  - PostgreSQL metadata DB

### ✅ Validates
- Uses **Soda SQL** for data quality checks

### ✅ Tests & Deploys
- Unit, integration, and end-to-end tests with **Pytest**
- CI/CD automation using **GitHub Actions**

---

## 🏗 Architecture Overview

```text
YouTube API
   ↓
Raw JSON (Bronze)
   ↓
PostgreSQL Staging
   ↓
Transformations + AI Sentiment
   ↓
PostgreSQL Core Tables
   ↓
Soda Data Quality Checks
   ↓
Airflow DAGs
   ↓
CI/CD (GitHub Actions)
🛠 Tech Stack
Language: Python

API: YouTube Data API v3

Orchestration: Apache Airflow

Warehouse: PostgreSQL

Containerization: Docker & Docker Compose

AI / NLP: HuggingFace Transformers

Data Quality: Soda SQL

Testing: Pytest

CI/CD: GitHub Actions

⚙️ Pipeline Workflow
Extract YouTube Data

Fetch all video IDs from MrBeast channel

Pull video metadata

Save raw JSON

Database Setup

PostgreSQL initialized via Docker

Staging and core tables created via hooks and cursors

Transformation Layer

Video duration → short / normal

Title sentiment + sentiment score

Timestamp normalization

Modification Layer

Insert, update, delete logic

Uses row dictionaries returned from transformations

Orchestration

Tasks defined and grouped in Airflow DAGs

Monitored via Airflow UI

Data Quality Checks

Soda scans executed post-load

🗂 Airflow DAG Flow
mermaid
Copy code
flowchart TD
    A[Trigger DAG] --> B[Extract YouTube API Data]
    B --> C[Save Raw JSON]
    C --> D[Load to PostgreSQL Staging]
    D --> E[Transform Data]
    E --> F[Video Type Logic]
    E --> G[Sentiment Analysis]
    F --> H[Core Table Insert / Update]
    G --> H
    H --> I[Soda Data Quality Scan]
    I --> J[Unit / Integration / E2E Tests]
    J --> K[DAG Success]
📂 Project Structure
text
Copy code
.
├── dags/
│   └── youtube_pipeline_dag.py
├── datawarehousing/
│   ├── data_utils.py          # DB hooks, connections, AI sentiment
│   ├── data_loading.py        # API extraction & raw loading
│   ├── transformation.py     # Business + AI transformations
│   ├── modification.py       # Insert / update / delete logic
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── e2e/
│   └── conftest.py
├── soda/
│   └── checks.yml
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
└── README.md
🧪 Data Quality & Testing
🔍 Soda Checks
No duplicate records

No missing critical columns

≥ 90% sentiment scores not zero

≥ 90% titles not neutral

🧪 Testing Levels
Unit Tests: DAG imports, mocks, DB connections

Integration Tests: Real API & PostgreSQL

End-to-End Tests: Full pipeline execution via pytest

🚀 CI/CD Workflow
Implemented using GitHub Actions

Conditional workflows based on file changes:

requirements.txt

DAGs

Soda configs

Docker files

Supports manual workflow dispatch

🔐 Environment & Secrets
.env files are not committed

Secrets stored in GitHub Secrets & Variables

Referenced as:

yaml
Copy code
${{ secrets.SECRET_NAME }}
Docker Compose refactored to read from GitHub secrets

⚡ How to Run
Clone the repository

Run docker-compose up -d

Open Airflow UI at http://localhost:8080

Trigger DAG

Run pytest for validation

✅ Final Outcome
✔ End-to-end YouTube ELT pipeline
✔ AI-enhanced analytics
✔ Production-grade Airflow orchestration
✔ Automated data quality & CI/CD
