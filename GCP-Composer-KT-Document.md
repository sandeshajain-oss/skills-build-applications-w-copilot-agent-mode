# GCP Composer Function - Knowledge Transfer Document

**Last Updated:** 2026-04-28  
**Author:** sandeshajain-oss  
**Status:** Complete

---

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Terraform Module Implementation](#terraform-module-implementation)
4. [Data Pipeline Workflow](#data-pipeline-workflow)
5. [Azure DevOps CI/CD Integration](#azure-devops-cicd-integration)
6. [Deployment Process](#deployment-process)
7. [Monitoring & Troubleshooting](#monitoring--troubleshooting)
8. [Best Practices](#best-practices)
9. [Common Issues & Solutions](#common-issues--solutions)
10. [Contact & Support](#contact--support)

---

## Overview

### What is GCP Composer?

Google Cloud Composer is a managed Apache Airflow service that orchestrates data workflows across Google Cloud and hybrid environments. In our implementation, it serves as the central orchestration engine for data pipeline management.

### Purpose

Our GCP Composer function automates the data movement process from **Raw Datasets** to **Insights Datasets** in BigQuery, enabling:
- Scheduled, reliable data transformations
- Monitoring and alerting capabilities
- Version control through Infrastructure-as-Code (Terraform)
- Automated deployments via Azure DevOps

### Key Benefits
- ✅ Fully managed Airflow environment (no infrastructure to manage)
- ✅ Native GCP integration (BigQuery, Cloud Storage, Pub/Sub)
- ✅ Horizontal scalability
- ✅ Built-in monitoring and logging
- ✅ Version control and audit trails

---

## Architecture

### High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    Azure DevOps CI/CD                        │
│              (Pipeline, Build, Release)                      │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│              Terraform Module (IaC)                          │
│     - GCP Composer Environment Setup                         │
│     - DAG Deployment Configuration                           │
│     - Service Account & IAM Roles                            │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│            GCP Cloud Composer                                │
│         (Managed Airflow Environment)                        │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  DAG: Data Pipeline Orchestration                    │  │
│  │  - Extract from Raw Datasets                         │  │
│  │  - Transform & Validate                              │  │
│  │  - Load to Insights Datasets                         │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────┬────────────────────────────────────────┘
                     │
        ┌────────────┴────────────┐
        ▼                         ▼
┌─────────────────┐      ┌──────────────────┐
│  BigQuery       │      │  Cloud Storage   │
│  Raw Datasets   │      │  (Staging/Logs)  │
└────────┬────────┘      └──────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  BigQuery                           │
│  Insights Datasets (Processed Data) │
└─────────────────────────────────────┘
```

### Components

| Component | Purpose | Technology |
|-----------|---------|-----------|
| **Orchestration** | Workflow scheduling & monitoring | Apache Airflow (GCP Composer) |
| **Infrastructure** | Define & provision resources | Terraform |
| **CI/CD Pipeline** | Automate deployment process | Azure DevOps |
| **Data Source** | Raw data storage | BigQuery (Raw Datasets) |
| **Data Sink** | Processed data storage | BigQuery (Insights Datasets) |
| **Staging** | Temporary data & logs | Cloud Storage |

---

## Terraform Module Implementation

### Directory Structure

```
terraform/
├── main.tf                 # Main Composer environment configuration
├── variables.tf            # Input variables
├── outputs.tf              # Output values
├── iam.tf                  # Service accounts & IAM roles
├── dag_deployment.tf       # DAG file configuration
├── environment.tfvars      # Environment-specific variables
└── modules/
    └── composer/
        ├── main.tf
        ├── variables.tf
        ├── outputs.tf
        └── dag_templates/
            └── etl_pipeline.py
```

### Sample Terraform Configuration

#### `main.tf` - Composer Environment

```hcl
module "cloud_composer" {
  source = "./modules/composer"

  project_id             = var.gcp_project_id
  composer_environment   = var.composer_env_name
  location               = var.gcp_region
  machine_type           = "n1-standard-4"
  node_count             = 3
  
  # Airflow Configuration
  airflow_config_overrides = {
    core-parallelism           = "32"
    core-max_active_runs_per_dag = "16"
    core-dag_file_processor_timeout = "50"
  }

  # Environment Variables
  env_variables = {
    BQ_RAW_DATASET         = var.bq_raw_dataset
    BQ_INSIGHTS_DATASET    = var.bq_insights_dataset
    GCS_STAGING_BUCKET     = var.gcs_staging_bucket
    ENVIRONMENT            = var.environment
  }

  # Network Configuration
  network_project_id = var.network_project_id
  subnetwork         = var.subnetwork

  # Logging
  enable_cloud_logging = true
  log_level            = "INFO"

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
    Team        = var.team
  }
}
```

#### `iam.tf` - Service Account Setup

```hcl
# Service Account for Composer
resource "google_service_account" "composer_sa" {
  account_id   = "composer-etl-${var.environment}"
  display_name = "GCP Composer ETL Service Account"
  project      = var.gcp_project_id
}

# Required Roles
locals {
  composer_roles = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectCreator",
    "roles/storage.objectViewer",
    "roles/logging.logWriter",
  ]
}

resource "google_project_iam_member" "composer_roles" {
  for_each = toset(local.composer_roles)
  
  project = var.gcp_project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

# IAM Binding for Composer Environments
resource "google_composer_environment_iam_member" "composer_worker" {
  environment = module.cloud_composer.composer_env_name
  location    = var.gcp_region
  role        = "roles/composer.worker"
  member      = "serviceAccount:${google_service_account.composer_sa.email}"
}
```

#### `variables.tf` - Input Variables

```hcl
variable "gcp_project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "composer_env_name" {
  description = "Name of the Cloud Composer environment"
  type        = string
}

variable "gcp_region" {
  description = "GCP region for Composer deployment"
  type        = string
  default     = "us-central1"
}

variable "bq_raw_dataset" {
  description = "BigQuery raw dataset name"
  type        = string
}

variable "bq_insights_dataset" {
  description = "BigQuery insights dataset name"
  type        = string
}

variable "gcs_staging_bucket" {
  description = "GCS bucket for staging and logs"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "team" {
  description = "Team responsible for this resource"
  type        = string
}
```

#### `dag_deployment.tf` - DAG Upload

```hcl
# Upload DAG to Composer environment
resource "google_storage_bucket_object" "etl_dag" {
  name            = "dags/etl_pipeline.py"
  bucket          = module.cloud_composer.composer_bucket_name
  source          = "${path.module}/dags/etl_pipeline.py"
  detect_mimetype = true
}
```

---

## Data Pipeline Workflow

### ETL Process Flow

```
Raw Datasets (BigQuery)
        │
        ▼
   ┌─────────────────────────────────────┐
   │  Extract Phase                      │
   │  - Query raw tables                 │
   │  - Apply data quality checks        │
   │  - Handle incremental loads         │
   └────────────┬────────────────────────┘
                │
                ▼
   ┌─────────────────────────────────────┐
   │  Transform Phase                    │
   │  - Apply business logic             │
   │  - Aggregate & Enrich data          │
   │  - Generate insights               │
   │  - Validate data quality            │
   └────────────┬────────────────────────┘
                │
                ▼
   ┌─────────────────────────────────────┐
   │  Load Phase                         │
   │  - Write to Insights Dataset        │
   │  - Update metadata                  │
   │  - Archive raw data (optional)      │
   └────────────┬────────────────────────┘
                │
                ▼
Insights Datasets (BigQuery)
```

### Sample DAG Implementation

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.task_group import TaskGroup
import os

# Environment variables
BQ_RAW_DATASET = os.getenv('BQ_RAW_DATASET', 'raw_data')
BQ_INSIGHTS_DATASET = os.getenv('BQ_INSIGHTS_DATASET', 'insights_data')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCS_STAGING_BUCKET = os.getenv('GCS_STAGING_BUCKET')

default_args = {
    'owner': 'data-engineering-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['data-team@company.com'],
    'start_date': datetime(2026, 1, 1),
}

dag = DAG(
    'raw_to_insights_etl',
    default_args=default_args,
    description='ETL pipeline: Raw datasets to Insights datasets',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC
    catchup=False,
    tags=['etl', 'bigquery', 'daily'],
)

# Data quality check SQL
data_quality_sql = f"""
SELECT COUNT(*) as record_count
FROM `{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.raw_customers`
WHERE created_date >= CURRENT_DATE() - 1
"""

# Transform SQL
transform_sql = f"""
INSERT INTO `{GCP_PROJECT_ID}.{BQ_INSIGHTS_DATASET}.customers_insights`
SELECT
    customer_id,
    customer_name,
    email,
    country,
    total_orders,
    total_revenue,
    last_order_date,
    CURRENT_TIMESTAMP() as processed_at,
    'ACTIVE' as status
FROM `{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.raw_customers`
WHERE created_date >= CURRENT_DATE() - 1
  AND customer_id IS NOT NULL
"""

with TaskGroup("data_quality", dag=dag) as data_quality_tg:
    check_raw_data = BigQueryInsertJobOperator(
        task_id='check_raw_data_quality',
        configuration={
            "query": {
                "query": data_quality_sql,
                "useLegacySql": False,
                "location": "US",
            }
        },
        project_id=GCP_PROJECT_ID,
    )

with TaskGroup("transformation", dag=dag) as transform_tg:
    transform_customers = BigQueryInsertJobOperator(
        task_id='transform_customers',
        configuration={
            "query": {
                "query": transform_sql,
                "useLegacySql": False,
                "location": "US",
                "destinationDataset": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BQ_INSIGHTS_DATASET,
                },
            }
        },
        project_id=GCP_PROJECT_ID,
    )

with TaskGroup("validation", dag=dag) as validation_tg:
    validate_insights = BigQueryInsertJobOperator(
        task_id='validate_insights',
        configuration={
            "query": {
                "query": f"""
                SELECT COUNT(*) as insight_records
                FROM `{GCP_PROJECT_ID}.{BQ_INSIGHTS_DATASET}.customers_insights`
                WHERE processed_at >= CURRENT_TIMESTAMP() - INTERVAL 1 DAY
                """,
                "useLegacySql": False,
                "location": "US",
            }
        },
        project_id=GCP_PROJECT_ID,
    )

# Set dependencies
data_quality_tg >> transform_tg >> validation_tg
```

---

## Azure DevOps CI/CD Integration

### Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Git Repository                             │
│         (Terraform + DAG files + Config)                     │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼ (Commit/PR)
┌─────────────────────────────────────────────────────────────┐
│              Azure DevOps Pipeline                            │
│                                                              │
│  Build Stage:                                               │
│  └─ Run Terraform validate & plan                          │
│  └─ Run terraform fmt check                                │
│  └─ Run security scanning                                  │
│  └─ Publish artifacts                                      │
│                                                              │
│  Test Stage:                                                │
│  └─ Deploy to dev environment                              │
│  └─ Run integration tests                                  │
│  └─ Validate DAG syntax                                    │
│                                                              │
│  Release Stage:                                             │
│  └─ Approval gate                                          │
│  └─ Deploy to staging environment                          │
│  └─ Manual approval for production                         │
│  └─ Deploy to production environment                       │
│                                                              │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
        ┌────────────┴────────────┐
        ▼                         ▼
   Dev Environment         Production Environment
   (Test & Validate)       (Live Deployment)
```

### Sample Azure DevOps Pipeline

#### `azure-pipelines.yml`

```yaml
trigger:
  branches:
    include:
      - main
      - develop
      - feature/*
  paths:
    include:
      - terraform/**
      - dags/**
      - .ci/**

pr:
  branches:
    include:
      - main
      - develop

pool:
  vmImage: 'ubuntu-latest'

variables:
  terraformVersion: '1.5.0'
  pythonVersion: '3.10'
  GCP_PROJECT_ID: $(GCP_PROJECT_ID)

stages:
  - stage: Build
    displayName: 'Build & Validate'
    jobs:
      - job: BuildJob
        displayName: 'Terraform & Code Validation'
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: '$(pythonVersion)'

          - task: TerraformInstaller@0
            inputs:
              terraformVersion: '$(terraformVersion)'

          - task: Bash@3
            displayName: 'Terraform Validate'
            inputs:
              targetType: 'inline'
              script: |
                cd terraform
                terraform init -backend=false
                terraform validate
                terraform fmt -check

          - task: Bash@3
            displayName: 'Terraform Plan (Dev)'
            inputs:
              targetType: 'inline'
              script: |
                cd terraform
                terraform init \
                  -backend-config="bucket=$(TF_BACKEND_BUCKET)" \
                  -backend-config="prefix=dev"
                terraform plan -var-file=environments/dev.tfvars -out=tfplan

          - task: Bash@3
            displayName: 'Security Scanning'
            inputs:
              targetType: 'inline'
              script: |
                pip install checkov
                checkov -d terraform --framework terraform

          - task: Bash@3
            displayName: 'Validate DAG Syntax'
            inputs:
              targetType: 'inline'
              script: |
                pip install apache-airflow
                python -m py_compile dags/*.py
                airflow dags validate dags/

          - task: PublishBuildArtifacts@1
            displayName: 'Publish Terraform Plan'
            inputs:
              PathtoPublish: 'terraform/tfplan'
              ArtifactName: 'tfplan'

  - stage: Test
    displayName: 'Deploy to Dev & Test'
    dependsOn: Build
    condition: succeeded()
    jobs:
      - deployment: DeployDev
        displayName: 'Deploy to Dev Environment'
        environment: 'dev'
        strategy:
          runOnce:
            deploy:
              steps:
                - task: DownloadBuildArtifacts@0
                  inputs:
                    artifactName: 'tfplan'

                - task: TerraformInstaller@0
                  inputs:
                    terraformVersion: '$(terraformVersion)'

                - task: Bash@3
                  displayName: 'Terraform Apply (Dev)'
                  inputs:
                    targetType: 'inline'
                    script: |
                      cd terraform
                      terraform apply -auto-approve tfplan

      - job: IntegrationTests
        displayName: 'Run Integration Tests'
        dependsOn: DeployDev
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: '$(pythonVersion)'

          - task: Bash@3
            displayName: 'Run DAG Tests'
            inputs:
              targetType: 'inline'
              script: |
                pip install apache-airflow pytest pytest-cov
                pytest tests/ -v --cov=dags

  - stage: Release
    displayName: 'Release to Production'
    dependsOn: Test
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - deployment: DeployStaging
        displayName: 'Deploy to Staging'
        environment: 'staging'
        strategy:
          runOnce:
            deploy:
              steps:
                - task: Bash@3
                  displayName: 'Terraform Apply (Staging)'
                  inputs:
                    targetType: 'inline'
                    script: |
                      cd terraform
                      terraform init -backend-config="prefix=staging"
                      terraform apply -var-file=environments/staging.tfvars -auto-approve

      - deployment: DeployProduction
        displayName: 'Deploy to Production'
        environment: 'production'
        dependsOn: DeployStaging
        strategy:
          runOnce:
            deploy:
              steps:
                - task: Bash@3
                  displayName: 'Terraform Apply (Production)'
                  inputs:
                    targetType: 'inline'
                    script: |
                      cd terraform
                      terraform init -backend-config="prefix=production"
                      terraform apply -var-file=environments/production.tfvars -auto-approve
```

### Variable Groups in Azure DevOps

Create the following variable groups in Azure DevOps:

**Group: `terraform-backend`**
- `TF_BACKEND_BUCKET`: gs://your-terraform-backend-bucket
- `GOOGLE_APPLICATION_CREDENTIALS`: /path/to/service-account-key.json

**Group: `gcp-credentials`**
- `GCP_PROJECT_ID`: your-project-id
- `GCP_REGION`: us-central1

---

## Deployment Process

### Step-by-Step Deployment Guide

#### Prerequisites
- [ ] GCP Project with Cloud Composer API enabled
- [ ] Service Account with necessary permissions
- [ ] Terraform backend (GCS bucket) configured
- [ ] Azure DevOps organization and project setup
- [ ] Git repository initialized

#### Local Deployment (Development)

```bash
# 1. Clone repository
git clone <repo-url>
cd gcp-composer-project

# 2. Initialize Terraform
terraform init

# 3. Validate configuration
terraform validate

# 4. Plan deployment
terraform plan -var-file=environments/dev.tfvars -out=tfplan

# 5. Review plan and apply
terraform apply tfplan

# 6. Verify Composer environment
gcloud composer environments describe <composer-env-name> \
  --location <region>

# 7. Monitor DAG deployment
gcloud composer environments storage dags list \
  --environment <composer-env-name> \
  --location <region>
```

#### Azure DevOps Pipeline Deployment

1. **Push Changes:**
   ```bash
   git add .
   git commit -m "Update Composer configuration"
   git push origin develop
   ```

2. **Create Pull Request (if required)**
   - Navigate to Azure Repos
   - Create PR to merge into `main`
   - Code review and approval

3. **Monitor Pipeline:**
   - Go to Azure Pipelines → Recent runs
   - Monitor Build stage → Test stage → Release stage
   - Review logs for any issues

4. **Production Deployment:**
   - Merge to `main` branch triggers production deployment
   - Manual approval gate required (if configured)
   - Monitor release progress

### Rollback Procedure

If deployment fails:

```bash
# 1. Check current state
terraform state list

# 2. Show previous state backup
ls -la terraform.tfstate*

# 3. Rollback to previous version
terraform destroy -var-file=environments/prod.tfvars

# 4. Restore previous configuration
git checkout <previous-commit-hash>

# 5. Re-apply previous state
terraform apply -var-file=environments/prod.tfvars
```

---

## Monitoring & Troubleshooting

### Monitoring DAG Execution

#### Using Cloud Console

1. Open Cloud Composer → Environments
2. Click on environment name
3. Click "Airflow webserver" link
4. Monitor DAG runs and task execution

#### Using gcloud CLI

```bash
# List recent DAG runs
gcloud composer environments run <env-name> \
  --location <region> \
  dags list

# Get DAG task status
gcloud composer environments run <env-name> \
  --location <region> \
  tasks list --dag_id raw_to_insights_etl

# View DAG logs
gcloud logging read "resource.type=cloud-composer-environment AND \
  jsonPayload.dag_id=raw_to_insights_etl" \
  --limit 50 \
  --format json
```

### Key Metrics to Monitor

| Metric | Threshold | Alert Action |
|--------|-----------|--------------|
| DAG Success Rate | < 95% | Investigate failure logs |
| Task Duration | 2x normal time | Check data volume, query performance |
| Composer CPU Usage | > 80% | Scale up node count |
| Composer Memory Usage | > 85% | Reduce parallelism or scale up |
| BigQuery Jobs Failed | Any failure | Review query syntax and data |

### Common Issues & Resolution

#### Issue 1: DAG Not Appearing in Airflow UI

**Symptoms:**
- DAG uploaded but not visible in Airflow webserver

**Causes:**
- DAG syntax error
- Missing required imports
- Incorrect file location

**Resolution:**
```bash
# Check DAG syntax
python -m py_compile dags/etl_pipeline.py

# Validate DAG with Airflow
airflow dags validate dags/etl_pipeline.py

# Check DAG location in Composer
gsutil ls gs://<composer-bucket>/dags/

# Review Airflow logs
gcloud logging read "resource.type=cloud-composer-environment" \
  --limit 100 --format json | grep -i "etl_pipeline"
```

#### Issue 2: BigQuery Access Denied

**Symptoms:**
- Tasks failing with "Permission denied" error

**Causes:**
- Service account lacks required permissions
- Dataset or table access not granted
- IAM roles not properly assigned

**Resolution:**
```bash
# Verify service account permissions
gcloud projects get-iam-policy <project-id> \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:*"

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding <project-id> \
  --member=serviceAccount:<sa-email> \
  --role=roles/bigquery.dataEditor

# Grant dataset access
gcloud bq datasets update <dataset> \
  --set_iam_policy=policy.json
```

#### Issue 3: Composer Environment Scaling Issues

**Symptoms:**
- Slow task execution
- Queue backlog
- High resource utilization

**Resolution:**
```bash
# Update node count
gcloud composer environments update <env-name> \
  --location <region> \
  --node-count 5 \
  --machine-type n1-standard-4

# Adjust Airflow parallelism
gcloud composer environments update <env-name> \
  --location <region> \
  --update-airflow-configs core-parallelism=64
```

#### Issue 4: Data Quality Validation Failures

**Symptoms:**
- DAG fails at data validation step

**Causes:**
- Unexpected data format
- Missing required columns
- Data volume exceeding threshold

**Resolution:**
```sql
-- Check raw data quality
SELECT 
  COUNT(*) as record_count,
  COUNT(DISTINCT customer_id) as unique_customers,
  COUNT(CASE WHEN customer_name IS NULL THEN 1 END) as null_names
FROM `project.raw_data.raw_customers`
WHERE created_date >= CURRENT_DATE() - 1;

-- Identify problematic records
SELECT *
FROM `project.raw_data.raw_customers`
WHERE customer_id IS NULL
  OR customer_name IS NULL
  OR email IS NULL
LIMIT 100;
```

---

## Best Practices

### Infrastructure & Configuration

1. **Environment Segregation**
   - Separate Composer environments for dev, staging, and production
   - Use distinct GCP projects for production
   - Implement network isolation using VPCs

2. **State Management**
   - Use remote Terraform backend (GCS)
   - Enable state locking and encryption
   - Maintain state backups

   ```hcl
   terraform {
     backend "gcs" {
       bucket         = "terraform-state-bucket"
       prefix         = "composer"
       encryption_key = var.backend_encryption_key
     }
   }
   ```

3. **Version Control**
   - Tag releases: `v1.0.0`, `v1.1.0`
   - Maintain CHANGELOG.md
   - Use semantic versioning

### DAG Design

4. **DAG Naming & Organization**
   - Use descriptive names: `raw_to_insights_etl`
   - Keep DAG file size < 300 lines
   - Group related tasks using TaskGroups

5. **Error Handling & Retries**
   ```python
   default_args = {
       'retries': 3,
       'retry_delay': timedelta(minutes=5),
       'retry_exponential_backoff': True,
       'max_retry_delay': timedelta(hours=1),
       'email_on_failure': True,
   }
   ```

6. **Idempotency**
   - Design tasks to be re-runnable
   - Use merge/upsert patterns instead of inserts
   - Implement "idempotent writes" in BigQuery

### Monitoring & Logging

7. **Comprehensive Logging**
   ```python
   import logging
   
   logger = logging.getLogger(__name__)
   logger.info(f"Processing {record_count} records")
   logger.warning(f"Found {error_count} data quality issues")
   ```

8. **Alerting Strategy**
   - Set up Pub/Sub notifications for failures
   - Configure email alerts for critical DAGs
   - Use Cloud Monitoring for infrastructure metrics

9. **Audit & Compliance**
   - Enable Cloud Audit Logs
   - Document all manual interventions
   - Maintain deployment audit trail

### Security

10. **Access Control**
    - Use least-privilege IAM roles
    - Implement service accounts per environment
    - Regularly rotate credentials

11. **Secrets Management**
    ```python
    from airflow.models import Variable
    
    db_password = Variable.get("db_password", deserialize_json=False)
    api_key = Variable.get("api_key", deserialize_json=False)
    ```

12. **Data Protection**
    - Encrypt data in transit (TLS)
    - Use Cloud Key Management Service (KMS) for encryption keys
    - Implement row-level security in BigQuery

---

## Common Issues & Solutions

### Performance Issues

**Issue:** Slow DAG execution
**Solution:**
```python
# Use batch operations instead of row-by-row
# Bad: Loop and insert
for row in data:
    insert_query(row)

# Good: Batch insert
insert_batch_query(data_chunk=1000)

# Use parallel task execution
BashOperator(
    task_id='parallel_task_1',
    pool='parallel_pool',
    pool_slots=2,
)
```

### Resource Constraints

**Issue:** Out of memory errors
**Solution:**
- Increase node count in Composer
- Reduce parallelism in Airflow config
- Optimize DAG memory usage

```bash
gcloud composer environments update <env-name> \
  --location <region> \
  --node-count 5
```

### Data Quality Issues

**Issue:** Incomplete or corrupted data in insights dataset
**Solution:**
```sql
-- Add data quality checks
CREATE OR REPLACE PROCEDURE `project.dataset.validate_data`()
BEGIN
  DECLARE row_count INT64;
  
  SET row_count = (
    SELECT COUNT(*)
    FROM `project.dataset.insights_data`
    WHERE processed_at >= CURRENT_TIMESTAMP() - INTERVAL 1 DAY
  );
  
  IF row_count < 1000 THEN
    RAISE USING MESSAGE = 'Insufficient data rows loaded';
  END IF;
END;
```

---

## Contact & Support

### Team Contacts

| Role | Name | Email |
|------|------|-------|
| Data Engineering Lead | - | data-lead@company.com |
| Cloud Infrastructure Lead | - | infra-lead@company.com |
| DevOps Engineer | - | devops@company.com |

### Documentation References

- [GCP Cloud Composer Documentation](https://cloud.google.com/composer/docs)
- [Apache Airflow Official Docs](https://airflow.apache.org/docs/)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Terraform Google Provider](https://registry.terraform.io/providers/hashicorp/google/latest)
- [Azure DevOps Documentation](https://docs.microsoft.com/en-us/azure/devops/)

### Useful Commands

```bash
# List all Composer environments
gcloud composer environments list --locations us-central1

# Describe Composer environment
gcloud composer environments describe <env-name> --location us-central1

# SSH into Composer node
gcloud composer environments ssh <env-name> --location us-central1

# Monitor DAG runs
gcloud composer environments run <env-name> \
  --location us-central1 \
  dags list-runs --dag_id raw_to_insights_etl

# Check Composer resource usage
gcloud composer environments describe <env-name> \
  --location us-central1 \
  --format="get(config.nodeCount, config.machineType)"

# View Composer logs
gcloud logging read \
  "resource.type=cloud-composer-environment AND \
   resource.labels.environment_name=<env-name>" \
  --limit 100 --format json
```

### Glossary

- **DAG:** Directed Acyclic Graph - represents workflow structure
- **Composer:** Google Cloud's managed Airflow service
- **BigQuery:** Google's serverless data warehouse
- **Terraform:** Infrastructure-as-Code tool for provisioning
- **Azure DevOps:** Microsoft's platform for development, deployment, and monitoring
- **ETL:** Extract, Transform, Load process
- **GCS:** Google Cloud Storage
- **IAM:** Identity and Access Management

---

**Document Version:** 1.0  
**Last Updated:** 2026-04-28  
**Maintained By:** Data Engineering Team
