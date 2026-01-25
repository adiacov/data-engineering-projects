# **Project P4 — Batch Orchestration with Apache Airflow**

## 1. Business Problem & Real-World Context

So far, you have **ingestion, transformation, and modeling** pipelines built in Python, executed manually.

In a real-world environment, batch pipelines must be:

* **Scheduled** (e.g., daily, weekly)
* **Monitored** for failures
* **Idempotent** (safe to rerun)
* **Backfillable** (handle missing historical runs)
* **Composable** (tasks can depend on each other)

Your task is to **orchestrate all previous steps** (P1 → P3) using **Apache Airflow**, simulating a small but realistic production workflow.

---

## 2. Input Data

* Use your **fact and dimension tables** from P3.
* No new datasets required.
* Assume daily incremental ingestion is optional, but orchestrate the full refresh pipeline first.

---

## 3. High-Level System Design

```
+----------------------+
|    Raw CSV Files     |
+----------+-----------+
           |
           v
+----------------------+
| P1: Batch Ingestion  |
+----------+-----------+
           |
           v
+----------------------+
| P2: Transformations  |
+----------+-----------+
           |
           v
+----------------------+
| P3: Data Modeling    |
+----------+-----------+
           |
           v
+----------------------+
| Analytical DB        |
|  (SQLite/DuckDB)     |
+----------------------+
           ^
           |
+----------------------+
|   Apache Airflow     |
|   DAG Scheduler      |
+----------------------+
```

---

## 4. Architecture Explanation

### **Components**

1. **Airflow DAG**

   * Defines tasks for each project stage (P1 → P3)
   * Encodes **dependencies** between tasks

2. **Tasks**

   * PythonOperator or BashOperator calling your scripts
   * Separate task per project stage
   * Logging and retries built-in

3. **Scheduler**

   * Runs DAG on a schedule (daily, or manual trigger)
   * Tracks task state and retries

4. **Metadata DB**

   * Airflow tracks DAG runs, task states, timestamps
   * Local SQLite used by default

---

## 5. Step-by-Step Milestones

### **Milestone 1 — Airflow Setup**

* Install Airflow locally (`pip install apache-airflow`)
* Initialize metadata DB
* Start webserver and scheduler for testing

---

### **Milestone 2 — DAG Definition**

* Create `dag_collisions_pipeline.py` in Airflow DAGs folder
* Define tasks for:

  * `ingest_raw`
  * `transform_clean`
  * `transform_curated`
  * `model_analytics`
* Set explicit dependencies (`ingest_raw >> transform_clean >> transform_curated >> model_analytics`)

---

### **Milestone 3 — Task Implementation**

* Each task executes the **existing Python scripts**
* Ensure **idempotency** (safe rerun)
* Add logging inside tasks (rows processed, errors, warnings)
* Implement **retry logic** for transient errors

---

### **Milestone 4 — DAG Testing**

* Trigger DAG manually
* Verify:

  * Task execution order
  * No duplication of processed data
  * Logs are informative
* Trigger DAG twice to confirm **idempotency**

---

### **Milestone 5 — Backfill Simulation**

* Simulate missing data for past dates
* Run DAG with backfill option
* Verify proper execution and correct updates

---

### **Milestone 6 — Observability**

* Ensure Airflow UI displays:

  * Task status (success, failed)
  * Run timestamps
  * Logs per task
* Optionally, add simple alerting (e.g., print errors to console)

---

## 6. Core Data Engineering Principles Involved

* Batch orchestration patterns
* Task dependency management
* Idempotent workflows
* Backfills and reruns
* Observability and logging

---

## 7. Open-Source Tools Used

* **Apache Airflow**

  * Scheduling, task orchestration, retries
* **Python scripts**

  * P1 → P3 pipeline steps
* **SQLite / DuckDB**

  * Analytical store for testing
* **SQL**

  * Queries for validation

No cloud services; entirely local.

---

## 8. Evaluation Checklist (Must-Pass)

### **Orchestration**

* [ ] DAG executes all tasks in correct order
* [ ] Tasks are modular and isolated
* [ ] Dependencies between tasks respected

### **Workflow Safety**

* [ ] Re-running DAG does not duplicate data
* [ ] Failed tasks can be retried successfully
* [ ] Backfill produces correct historical results

### **Observability**

* [ ] Logs show meaningful messages
* [ ] Airflow UI displays correct task states
* [ ] Errors are detectable

### **Engineering Discipline**

* [ ] Clear DAG naming
* [ ] Tasks call scripts, do not duplicate logic
* [ ] README accurately documents DAG behavior

---

## 9. Optional Extensions (Locked Until Passing)

* Parameterize DAG for multiple years
* Dynamic task generation for multiple datasets
* Add email alerts or Slack notifications
* Unit-test DAGs using Airflow test framework
