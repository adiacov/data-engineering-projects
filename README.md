# Data Engineering Learning Projects

#### Contents

- [Purpose](#purpose)
- [Learning Philosophy](#learning-philosophy)
- [Technology Constraints](#technology-constraints)
- [Repository Structure](#repository-structure)
- [Intended Outcome](#intended-outcome)
- [Full Project Roadmap (List)](#full-project-roadmap-list)
- [Roadmap (ASCII Graph)](#roadmap-ascii-graph)
- [Requirements](#requirements)
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
- Tools: Open-source, Free (e.g., Apache Airflow, Apache Spark, dbt, uv)

---

## Repository Structure

```yaml
data-engineering-projects/
├── README.md
├── src
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

## Requirements

- **Python ≥ 3.13**
- **uv tool** (to install, see instructions below)
- Linux OS (adjust paths if using Windows/macOS)

To install `uv`:

```bash
# For Linux/macOS
python3 -m pip install --upgrade uv
# Or check official instructions: https://uv.dev
```

---

## Package Python project

```bash
make package

# OR using `uv` directly
uv build --clear --wheel
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

# 3. At this point if you have 'uv' installed, you're all setup. Ypu can proceed to Run commands (see bellow).
#    (Optional) You can update the project's environment to synchronize python environment
uv lock
uv sync
```

## Deploy (Airflow Docker - starting from project_4)

```bash
# 1. Build the Airflow Docker image including project
make docker-build

# OR using docker compose directly
docker compose build
```

---

## Run

This is a python project, so make the usual staff.
From the project root (example for `/data-engineering-projects`):

``` python
# run a program (example for main_p1.py, but choose one if multiple at the same level)

# Run project 1
uv run p1

# Run project 2
uv run p2

# Run project xwz...
# uv run xwz (see pyproject.toml / [project.scripts])
```

The list of available commands you can find in the `pyproject.toml` under `[project.scripts]`.

You can also run scripts directly via Python if needed:

``` python
python3 -m src.de_project.main_p1

# Or
python3 ./src/de_project/main_p1.py

```

## Run (Airflow DAGs - starting from project_4)

```bash
# 1. Start Airflow containers in detached mode
make docker-up
#OR
docker compose up -d


# 2. Access the Airflow UI at:
#    http://localhost:8080
#    User: airflow
#    Password: airflow

# 3. Trigger DAGs manually or schedule them via the UI
# e.g. DAG dag_collisions_pipeline. See `dag_id` or `dag_display_name` in the DAG declaration. 

# 4. Stop and remove Airflow containers when done
make docker-down
#OR
docker compose down

```
