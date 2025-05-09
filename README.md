# Customer Lifetime Value (CLV) and Churn analysis
## An end-to-end data engineering pipeline  

[![status](https://img.shields.io/badge/status-pre--alpha-orange)]()
[![license](https://img.shields.io/badge/license-MIT-blue)]()
[![contributions](https://img.shields.io/badge/contributions-welcome-brightgreen)]()

## 1. Overview
This project is an end-to-end data engineering solution designed to help businesses quantify customer value, proactively identify high-risk, high-value segments, and generate actionable insights for revenue retention by reducing churn. The pipeline is architected with modularity, including quality checks at critical transformation layers, zone-based schema design, and extensible scaffolding for future data governance needs such as CI/CD integration, observability, cloud migration, and real-time streaming ingestion support.

### Business Problem:
Customer retention is one of the most impactful levers for sustainable revenue growth, yet many organizations lack reliable indicators to act on churn risk. Without a data-driven view of customer value and lifecycle signals, retention strategies become reactive and inefficient.

### Key Insight-Surfacing Questions:
* Who are the most valuable customers?
* Which customers signal the highest churn risk?
* What patterns in the customer lifecycle signal potential churn or long-term loyalty?
* How much revenue and projected revenue is currently at risk due to likely churn?

### Key Deliverables:
* CLV scoring for individual customers and value-based cohorts
* Churn risk scores and revenue-at-risk estimates
* Segmentations by lifecycle stage, contract type, customer behavior, and value
* Final data marts for downstream analytics and dashboards

### Key Technical Components:
* `Dagster`(orchestration) + `Airbyte`(ingestion) + `dbt`(transformation) + `DuckDB` (warehouse) + `Docker` (deployment)
* Modular ELT medallion architecture via `dbt` with `stg_`, `int_`, and `mart_` layers
* Asset-based DAG tracking and orchestration via `Dagster`
* Documented schema lineage and `dbt` testing suite
* Extendable design ready for governance, observability, cloud deployment, and CI/CD integrations

─────────────────────────────────────────────────────────────────────────────────────────
## 2. Project Architecture
The pipeline is organized into four logical data zones: Raw, Cleaned, Transformed, and Served.

### Tech stacks used:
* Airbyte for incremental ingestion loading support
* Dagster for orchestration and scheduling
* dbt for modular data modeling
* DuckDB as the local analytical warehouse
* Metabase and Streamlit as the frontend for analytics delivery

The diagram below illustrates the core architecture:
[insert diagram image here]

### Zone Definitions and Flow
* Raw: CSV files and metadata are incrementally ingested into DuckDB using Airbyte. Great Expectations validates schema, nullability, and basic distributions at this stage.
* Cleaned: dbt src_ and stg_ models normalize, typecast, and standardize raw inputs into clean analytical tables stored in DuckDB’s staging schema.
* Transformed: Business logic and feature engineering are applied via dim_, fct_, dbt models, producing required outputs such as CLV, churn flags, and customer segments. Additional data quality assertions are applied here via Great Expectations.
* Served: Final business aggregated marts tables are exposed to Metabase and Streamlit for no-tech stakeholder access, giving rise to actionable insights.

### Supporting Infrastructure
* Dagster is used to orchestrate all stages, providing dependency-aware asset scheduling, failure recovery, and audit logging.
* GitHub Actions CI/CD automates validation, dbt testing, and deployment of pipeline changes.
* Monte Carlo monitors table freshness, schema drift, and volume anomalies across Cleaned, Transformed, and Served zones to ensure data reliability for stream analytical tasks.
* Docker provides a consistent local runtime for reproducibility in different environments.

─────────────────────────────────────────────────────────────────────────────────────────
## 3. Source Data Overview
This project ingests a synthetic dataset representing 100,000 individual customer records that simulate the real-world data domains of a modern B2C business. Together they expose 58 raw features spanning demographics, financial posture, transactional activity, loyalty behavior, product-usage intensity, and support experience. This layout mirrors real-world data assets for building production-grade pipelines that power Customer Lifetime Value (CLV) modeling, Churn-risk scoring, Segmentation and cohort retention strategies.

### Dataset Strengths
* No actual personally identifiable information (PII), safe for publishing and collaborations
* Covers:
	* CLV modeling scope (orders + subscriptions + loyalty)
	* Churn scoring (inactivity, support friction, plan status)
	* Cohort analysis (demographics, credit segments, engagement depth)
* Well-balanced schema: good mix of wide and long features
* Mirrors real-world operational realities

### Dataset Schema
All files are delivered as CSV with UTF-8 encoding, comma delimited, with header rows.

customers.csv (CRM APIs)
| Column Name        | Type      | Description                 |
|  |  |  |
| customer_id        | STRING    | Primary key                 |
| first_name         | STRING    |                             |
| last_name          | STRING    |                             |
| email              | STRING    |                             |
| gender             | STRING    | M / F / Other / Unknown     |
| date_of_birth      | DATE      |                             |
| signup_datetime    | TIMESTAMP | Account-creation timestamp  |
| city               | STRING    | From latest profile address |
| country            | STRING    | ISO-3166                    |
| marketing_opt_in   | BOOLEAN   | GDPR/CCPA compliant flag    |

financial_profiles.csv (Credit-bureau exports)
| Column Name              | Type    | Description                   |
|  | - | -- |
| customer_id              | STRING  | FK to customers               |
| profile_effective_date   | DATE    | When score/income fetched     |
| income_usd               | DECIMAL | Annual income (self-reported) |
| credit_score             | INTEGER | 300-850 range                 |

orders.csv (E-commerce APIs)
| Column Name             | Type      | Description                    |
| -- |  |  |
| order_id                | STRING    | Primary key                    |
| customer_id             | STRING    | FK to customers                |
| order_datetime          | TIMESTAMP | Event time                     |
| product_id              | STRING    |                                |
| product_category        | STRING    | Normalized category            |
| quantity                | INTEGER   |                                |
| unit_price_usd          | DECIMAL   | Captured at transaction time   |
| device_type             | STRING    | web / ios / android            |
| order_channel           | STRING    | site / app / phone             |
| city                    | STRING    | Shipping city                  |
| country                 | STRING    | Shipping country               |
| loyalty_points_earned   | INTEGER   | Raw points granted on purchase |

loyalty_balances.csv (Loyalty system snapshot)
| Column Name     | Type    | Notes                           |
|  | - | - |
| customer_id     | STRING  | FK to customers                 |
| snapshot_date   | DATE    | Daily end-of-day snapshot       |
| points_balance  | INTEGER | Current total points            |
| loyalty_tier    | STRING  | Silver / Gold / Platinum / etc. |

subscriptions.csv (Billing/ERP APIs)
| Column Name      | Type    | Notes                       |
| - | - |  |
| subscription_id  | STRING  | Primary key                 |
| customer_id      | STRING  | FK to customers             |
| plan_id          | STRING  | Pricing-plan reference      |
| plan_name        | STRING  | Human-readable              |
| status           | STRING  | active / cancelled / paused |
| start_date       | DATE    | Service start               |
| end_date         | DATE    | Service end (if cancelled)  |
| renewal_date     | DATE    | Next scheduled renewal      |
| is_auto_renew    | BOOLEAN | True if auto-renew enabled  |

support_tickets.csv (Customer-support exports)
| Column Name               | Type      | Notes                                 |
| - |  | - |
| ticket_id                 | STRING    | Primary key                           |
| customer_id               | STRING    | FK to customers                       |
| opened_datetime           | TIMESTAMP |                                       |
| closed_datetime           | TIMESTAMP | Null if still open                    |
| status                    | STRING    | open / pending / closed               |
| channel                   | STRING    | email / chat / phone / social         |
| issue_type                | STRING    | billing / technical / account / other |
| resolution_time_minutes   | INTEGER   | Populated on close                    |
| csat_score                | SMALLINT  | 1–5 survey rating                     |

device_sessions.csv (Web / Mobile Logs)
| Column Name        | Type      | Notes                     |
|  |  | - |
| session_id         | STRING    | Primary key               |
| customer_id        | STRING    | FK to customers           |
| session_start_ts   | TIMESTAMP |                           |
| session_end_ts     | TIMESTAMP |                           |
| device_type        | STRING    | desktop / mobile / tablet |
| os_version         | STRING    | e.g. iOS 17, Android 14   |
| app_version        | STRING    | If mobile app             |
| pages_viewed       | INTEGER   |                           |
| events_fired       | INTEGER   | Count of tracked events   |

─────────────────────────────────────────────────────────────────────────────────────────
## 4. Pipeline Architecture & Data Modeling Plan
To demonstrate end-to-end data modeling strategies aligned with stakeholder requirements, we begin with final BI-ready marts that address stakeholders' business questions and trace upstream to the necessary raw inputs.

### Business-Driven Mart Design
Key business questions that guide the data mart design:
* Who are the most valuable customers?
* Which customers are at risk of churning?
* What patterns drive retention or churn across customer lifecycles?
* How much projected revenue is at risk due to churn?
* How do engagement, loyalty, and support servicing interactions relate to churn?

### Final Data Products (`mart_*`)
Table, Grain, Business Insight
* `mart_clv`, Customer, Most valuable customers.
* `mart_churn_risk`, Customer, Customers at risk; revenue at risk (uses `int_revenue_risk`).
* `mart_customer_segments`, Customer, Segments by lifecycle, contract, geo, plan, favorite category.
* `mart_retention_cohorts`, Cohort × months_since_acq, Retention curves & drivers.
* `mart_category_ltv`, Category, LTV & purchase frequency by favorite category.
* `mart_churn_support_analysis`, Customer or cohort, Churn patterns vs. support interactions.
* `mart_customer_360`, Customer(Wide), One-stop CLV, churn score, lifecycle, key engagement metrics for analysts.

### Intermediate Features (`int_*`)
Table, Description
* `int_customer_base`, Latest SCD-2 slice for each customer (joins dims & last snapshot).
* `int_customer_clv`, Lifetime revenue, tenure months, avg order value.
* `int_Engagement_summary`, Rolling 30/90-day activity counts, last session date, device mix.
* `int_support_summary`, Tickets count, avg resolution, CSAT of last 30/90 days.
* `int_lifecycle_stage`, Tags: onboarding, active, dormant, churned.
* `int_churn_scoring`, Churn probability using inactivity, support, tenure, lifecycle.
* `int_revenue_risk`, `churn_score × predicted_remaining_clv` per customer.
* `int_customer_category_pref`, Favorite category, spend share, frequency.

### Facts (`fct_*`)
Table, Grain, Features
* `fct_revenue`, revenue event, `customer_id`, `event_ts`, `revenue_usd`, `revenue_type` (“order”, “subscription”, “refund”).
* `fct_activity_logs`, session, Device, channel, duration, `dropoff_flag`.
* `fct_support_logs`, ticket event, Status changes, resolution time, CSAT.
* `fct_loyalty`, points event, Points earned, redeemed, source.
* `fct_customer_snapshot`, customer × date(Daily), Current status, current plan, balance, flags (active, churned).

### Dimensions (`dim_*`)
Table, Grain, Description
* `dim_time`, date, Calendar, fiscal flags.
* `dim_geography`, geo_id, Country → state → city hierarchy.
* `dim_products`, product_id, Includes category, cost, margin.
* `dim_plans`, plan_id, Billing cycle, list price, discount tiers.
* `dim_customers`, SCD-2 customer_id × version, Master profile; `effective_from`, `effective_to`, `is_current`.

### Supporting Layers
* `src_`, Source, Airbyte-landed raw files, CDC tables.
* `stg_`, Staging, Type-casted, de-duplicated, soft filters; one table ≈ one raw file.

### Lineage Snapshot
src_ → stg_ → dim_ + fct_ 
        └──→ int_* (feature engineering)
                └──→ mart_* (stakeholder-facing views)

### Warehouse Entity Relationship Diagrams

> View the interactive ERD on [dbdiagram.io](https://dbdiagram.io/d/681cb1575b2fc4582fc52602)

[insert ERD image here]

─────────────────────────────────────────────────────────────────────────────────────────
## 5. Dagster Orchestration
This project uses Dagster as the orchestration tool to manage data dependencies, enforce pipeline reliability, and operationalize the data flows from ingestion to BI-ready marts.

### Core Orchestration Tasks
* Dependency Management: Track and enforce upstream/downstream relationships across ingestion, modeling, and serving.
* Pipeline Scheduling: Refresh of all raw→staging→mart layers automatically.
* Asset Materialization: dbt models as first-class Dagster assets, enabling lineage introspection and retry handling.
* Failure Recovery: Catch failures early during ingestion, model execution, or testing, and rerun only the failed parts to save on compute.
* Monitoring: Raise alerts via `Great Expectations` and `Monte Carlo` when human interventions are required.
* Observability: Log status, duration, and freshness of each asset to debug outages and regressions.

### Asset Planning
All Dagster assets are planned around key modular components, mapped roughly to src_, stg_, int_, fct_, and mart_.
Each asset will:
* Reads a dataset or executes a dbt model
* Declares dependencies on other assets
* Emits logs and metadata
* Will be scheduled or triggered independently

### Pipeline Schedules
* daily_asset_refresh at Daily 2:30am: Runs all ingestion, staging, and marts with freshness check
* dbt_test_runner at Daily 3:30am: Runs all dbt schema and custom tests

### Quality Integrations
* Great Expectations: Schema, nulls, distributions checks
* dbt tests: Unique, not-null, accepted value, and logic tests
* Monte Carlo: Anomaly detection, lineage monitoring
* GitHub Actions: CI checks for dagster & dbt at code pushes

### Common Failures Handling
* Source extract fails
	* Action: Exponential back-off retry; escalate after *3* retries
	* Fix: Better monitor of upstream SLAs; set up secret rotation alarms; auto-backfill on recovery
* Schema drift
	* Action: Quarantine batch to `raw_invalid`; pause downstream
	* Fix: Enforce data contracts; allow column evolution with compatibility flags
* Data-quality regression
	* Action: Redirect batch to DLQ; notify data steward
	* Fix: Tune Great Expectations/dbt tests; build “quarantine → reprocess” workflow
* Duplicate / replayed data
	* Action: Run de-dup logic
	* Fix: Better idempotent loaders; atomic file-renames for batch loads
* Late-arriving / out-of-order events
	* Action: Use grace-period windows; re-run incremental loads
	* Fix: Tune event-time windows and watermarking (or Structured Streaming)
* Transform model fails
	* Action: Retry once; raise alert on second failure; roll back writes or use table swap
	* Fix: Tune SQL unit tests, autoscaling and resource limits
* Slow downstream queries / SLA miss
	* Action: `VACUUM` / `ANALYZE`; pre-materialize heavy joins
	* Fix: Tune schedule optimizations, partitioning, and clustering strategies
* DAG dependency mis-order
	* Action: Skip downstream; re-run after upstream fixed
	* Fix: Implement data-aware scheduling; check DAG code review procedures
* Resource exhaustion / cost spike
	* Action: Kill runaway job; temporarily scale node pool to accommodate
	* Fix: Add count guards and quotas; Tune cost anomaly alerts and autoscaling policies
* Bad Backfill
	* Action: Halt job; rollback, time-travel or snapshot
	* Fix: Use isolated schemas for backfills; adopt blue-green swap strategy

─────────────────────────────────────────────────────────────────────────────────────────
## 6. Project Structure
[Insert Project Folder File Trees here (ascii)]

─────────────────────────────────────────────────────────────────────────────────────────
## 7. Roadmap
* ☐ Phase 1: Foundation & Ingestion
* ☐ Phase 2: Modeling & Feature Engineering
* ☐ Phase 3: Orchestration & Testing
* ☐ Phase 4: Data Products & BI Delivery
* ☐ Phase 5: Enterprise-Ready Extensions

─────────────────────────────────────────────────────────────────────────────────────────
## 8. Conclusion
This end-to-end data engineering project demonstrates the full workflow for architecturing and implementing Customer Lifetime Value (CLV) and Churn analysis solutions, built with modularity, scalability, extensibility, and production-readiness in mind. The project's core objectives are:
* To proactively identify high-value customers at risk of churn
* To surface actionable insights that support real retention business strategies
* To model data systems that align with real-world business operational realities

Throughout the project, I’ve applied principles and practices that reflect modern data stack best practices:
* **Centrally Orchestrated DAGs**
* **Layered ELT data modeling**
* **Asset-centric lineage tracking and observability**
* **Future governance scaffolding**
* **BI-ready data marts**

### For Recruiters & Data Teams

If your data team needs to build data platforms that support high-impact decision-making, and/or needs to maintain existing data platforms for compliance, reliability, observability, quality assurance, and CICD best practices, I’d love to bring my architectural thinking, engineering discipline, and enthusiasm for healthy data systems to your team.

### Contacts

* **GitHub**: [github.com/Creative-Ataraxia](https://github.com/Creative-Ataraxia)
* **LinkedIn**: [linkedin.com/in/royma](https://www.linkedin.com/in/royma/)
* **Email**: [roy.ma9@gmail.com](mailto:roy.ma9@gmail.com)

