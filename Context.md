# Context
- Window 11, already setup wsl, Docker Deskstop and using VsCode with Antigravity CLI, Git have also been installed.
- MySQL have already been installed natively on windows (local instance MySQL 96 with root user at localhost:3306) & already setup the Sakila example Database/Dataset.

# Instruction
Act as a Senior Data Engineer and Mentor. Guide me end-to-end through a hands-on project to migrate the Sakila OLTP database (MySQL) to BigQuery OLAP, incorporating strict GitHub best practices throughout the development lifecycle.

The project must implement and cover the following requirements:

1. Data Architecture & Modeling:
- Design a Star Schema using the Medallion Architecture (Bronze -> Silver -> Gold).
- Implement advanced modeling techniques: Bridge Table, Conformed Dimension, Pre-aggregation, One-Big-Table (OBT), and Data Mart.
- Handle Slowly Changing Dimensions (SCD Type 2) for incoming periodic new data updates from the Sakila OLTP source.

2. Pipeline & Tooling:
- Build the OLAP data pipeline using Airflow and dbt (via astronomer-cosmos).
- Leverage dbt’s incremental materialization for efficient data processing.

3. Optimization & Reliability:
- Apply performance optimization solutions, including Indexing, Partitioning, and Clustering Keys.
- Ensure pipeline Idempotency and Fault Tolerance by implementing retry + exponential backoff, and robust error handling (e.g., handling wrong data types, etc.).

4. GitHub Workflow & Best Practices:
- Simulate a collaborative team workflow: guide me on when to create a new branch, commit, push, pull, open Pull Requests (PRs), and merge after each phase.


Since the Sakila OLTP database (running on MySQL) is the default dataset from MySQL there are no new data, create pipeline to create new data daily (don't need too much data, just need a few to simulate new incoming data).

Also the Bronze layer in BigQuery of this project should not be the same as Staging table (i.e. raw data from OLTP), they should be seperate, in Bronze layer we should perform type casting, and other similar practices (connect through dbt source function).

The DAG in Airflow should use Trigger at the end of each DAG (i.e. DAG for generation of data -> DAG migration -> DAG for dbt OLAP transformation)

For the section "1. Data Architecture & Modeling:" specifically, only give me guidance (i.e. what do to & relevent library, function) instead of the full answer

# Hard-requirement
- The guide must be beginner-friendly, suitable for a Fresherly graduated Data Engineer with next to no experience
- Please break this project down into clear, structured, step-by-step milestones. For each milestone, provide the architecture logic, configuration/code snippets (Airflow/dbt), and the exact Git commands I should execute.