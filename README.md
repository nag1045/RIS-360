# RIS-360
“The platform is built on a medallion architecture using S3 as the data lake. Data is processed using Glue PySpark jobs across bronze, silver, and gold layers. Query access is provided through Athena workgroups, and curated data is served from Redshift.


                ┌────────────────────┐
                │   GitHub Actions   │
                │   (CI / CD)        │
                └─────────┬──────────┘
                          │
                   AWS CDK (Python)
                          │
┌───────────────────────────────────────────────────────────┐
│                          AWS                              │
│                                                           │
│  ┌──────────────┐       ┌────────────────────────────┐  │
│  │     S3       │       │        EC2                  │  │
│  │ landing      │       │  Apache Airflow             │  │
│  │ bucket       │◀──────│  (Orchestration)            │  │
│  └──────┬───────┘       └───────────┬────────────────┘  │
│         │                            │                   │
│         ▼                            ▼                   │
│  ┌──────────────┐       ┌────────────────────────────┐  │
│  │     S3       │       │        AWS Glue             │  │
│  │ bronze       │◀──────│  PySpark ETL Jobs           │  │
│  │ bucket       │       └───────────┬────────────────┘  │
│  └──────┬───────┘                   │                   │
│         ▼                            ▼                   │
│  ┌──────────────┐       ┌────────────────────────────┐  │
│  │     S3       │       │        AWS Athena           │  │
│  │ silver       │◀──────│  (4 Workgroups)             │  │
│  │ bucket       │       │  Landing / Bronze /         │  │
│  └──────┬───────┘       │  Silver / Gold              │  │
│         ▼               └───────────┬────────────────┘  │
│  ┌──────────────┐                   │                   │
│  │     S3       │                   ▼                   │
│  │ gold         │        ┌──────────────────────────┐  │
│  │ bucket       │ ─────▶ │     Amazon Redshift       │  │
│  └──────────────┘        │   (Serving Layer)         │  │
│                           └──────────────────────────┘  │
│                                                           │
└───────────────────────────────────────────────────────────┘
