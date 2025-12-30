# Data Engineering Learning Projects

#### Contents

- [Purpose](#purpose)
- [Learning Philosophy](#learning-philosophy)
- [Technology Constraints](#technology-constraints)
- [Repository Structure](#repository-structure)
- [Intended Outcome](#intended-outcome)
- [Full Project Roadmap (List)](#full-project-roadmap-list)
- [Roadmap (ASCII Graph)](#roadmap-ascii-graph)
- [Deploy](#deploy)
- [Run](#run)

---

## Purpose

This repository contains a series of **hands-on data engineering projects** built to strengthen core data engineering fundamentals through **realistic, end-to-end systems**.

The projects follow a **bottom-up learning approach**:

- Start with simple batch data pipelines
- Progress to streaming systems
- Combine batch and streaming in later projects

Each project is designed to be **self-contained**, **locally runnable**, and based entirely on **free and open-source tools**.

---

## Learning Philosophy

- Learn by building real systems
- Work on **one project at a time**
- Move forward only after completing clear **evaluation criteria**
- Focus on understanding **why** systems are designed in a certain way

---

## Technology Constraints

- OS: Linux
- Language: Python
- Tools: Open-source, Free (e.g. Apache Airflow, Apache Spark, dbt)

---

## Repository Structure

```yaml
data-engineering-projects/
├── README.md
├── project-01-...
├── project-02-...
├── project-03-...
└── ...
```

Each project directory contains:

- A clear problem statement
- A simple ASCII system design
- Implementation steps
- Evaluation checklist
- Optional extensions

---

## Intended Outcome

By completing these projects, the goal is to:

- Build a strong foundation in data engineering principles
- Gain practical experience with batch and streaming systems
- Develop the ability to design, implement, and reason about data pipelines

This repository represents an **ongoing learning journey**, not a finished product.

Understood. I will act as a data engineering mentor and strictly follow your rules.

Below is the **complete, ordered project roadmap**, designed bottom-up and intended to make you comfortable in a real data engineering role using **Python on Linux**, with **hands-on local projects only**.

I will **keep this roadmap as the fixed reference** for all future steps.

---

## Full Project Roadmap (List)

### **Batch Foundations**

**[P1] Local File-Based Batch Ingestion**

- Ingest raw data from public datasets (CSV/JSON)
- Validate, normalize, persist to a local analytical store

**[P2] Batch Transformations & Data Quality**

- Transform raw → clean → curated layers
- Add schema enforcement, null handling, deduplication

**[P3] Analytical Data Modeling**

- Star/snowflake schema
- Fact & dimension tables
- Analytical queries and metrics

**[P4] Batch Orchestration**

- Schedule and monitor pipelines
- Idempotency and backfills
- Introduce Apache Airflow

---

### **Advanced Batch Systems**

**[P5] Distributed Batch Processing**

- Large-scale transformations
- Partitioning and performance
- Apache Spark (local mode)

**[P6] Analytics Engineering Layer**

- Transformations as code
- Tests, documentation
- Introduce dbt

---

### **Streaming Foundations**

**[P7] Streaming Ingestion**

- Event-based data flow
- Kafka + producers/consumers
- Simple persistence

**[P8] Streaming Processing**

- Stateful processing
- Windowing and aggregations
- Spark Structured Streaming

---

### **Advanced Streaming**

**[P9] Streaming Reliability & Semantics**

- Exactly-once concepts
- Late data, watermarking
- Reprocessing strategies

---

### **Hybrid Systems**

**[P10] Lambda-Style Hybrid Pipeline**

- Batch + streaming convergence
- Unified analytics layer
- Backfill + real-time reconciliation

---

## Roadmap (ASCII Graph)

```yaml
- [P1] File-based Batch Ingestion
   ↓
- [P2] Batch Transformations & Data Quality
   ↓
- [P3] Analytical Data Modeling
   ↓
- [P4] Batch Orchestration (Airflow)
   ↓
- [P5] Distributed Batch Processing (Spark)
   ↓
- [P6] Analytics Engineering (dbt)
   ↓
- [P7] Streaming Ingestion (Kafka)
   ↓
- [P8] Streaming Processing (Spark Streaming)
   ↓
- [P9] Streaming Reliability & Semantics
   ↓
- [P10] Hybrid Batch + Streaming Pipeline
```

---

## Deploy

Note: This are Linux instructions. Adjust to your OS.

To set up any project locally and make it available for development:

```bash
# 1. Clone the repository
git clone <repository_url>
cd <project-directory>

# 2. (Optional) Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Install the project in editable mode
pip install -e .
```

---

## Run

From the project root (example for `/data-engineering-projects/project-p1`):

```bash
python3 -m p1.main
```
