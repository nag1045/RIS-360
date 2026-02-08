# RIS-360
“The platform is built on a medallion architecture using S3 as the data lake. Data is processed using Glue PySpark jobs across bronze, silver, and gold layers. Query access is provided through Athena workgroups, and curated data is served from Redshift.

                ┌────────────────────┐
                │   GitHub Actions   │
                │      (CI / CD)     │
                └─────────┬──────────┘
                          │
                   AWS CDK (Python)
                          │
┌──────────────────────────────────────────────────────────────┐
│                           AWS                                │
│                                                              │
│  ┌──────────────┐       ┌──────────────────────────────┐   │
│  │     S3       │       │            EC2               │   │
│  │  Landing     │◀──────│      Apache Airflow          │   │
│  │   Bucket     │       │        (Orchestration)       │   │
│  └──────┬───────┘       └───────────┬──────────────────┘   │
│         │                            │                      │
│         ▼                            ▼                      │
│  ┌──────────────┐       ┌──────────────────────────────┐   │
│  │     S3       │       │          AWS Glue             │   │
│  │   Bronze     │◀──────│      PySpark ETL Jobs         │   │
│  │   Bucket     │       └───────────┬──────────────────┘   │
│  └──────┬───────┘                   │                      │
│         ▼                            ▼                      │
│  ┌──────────────┐       ┌──────────────────────────────┐   │
│  │     S3       │       │          AWS Athena           │   │
│  │   Silver     │◀──────│        (4 Workgroups)         │   │
│  │   Bucket     │       │   Landing / Bronze /          │   │
│  └──────┬───────┘       │   Silver / Gold               │   │
│         ▼               └───────────┬──────────────────┘   │
│  ┌──────────────┐                   │                      │
│  │     S3       │                   ▼                      │
│  │    Gold      │        ┌────────────────────────────┐   │
│  │   Bucket     │ ─────▶ │        Amazon Redshift      │   │
│  └──────────────┘        │       (Serving Layer)       │   │
│                           └────────────────────────────┘   │
│                                                              │
└──────────────────────────────────────────────────────────────┘
