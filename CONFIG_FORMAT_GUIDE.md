# Configuration Format - Bank Customer Risk Triggers ETL

## ✅ CORRECT CONFIGURATION STRUCTURE

The configuration now follows your exact template format with:

### **Top-Level Structure**
```yaml
generalInfo:           # Global application settings
mainFlow:             # Array of transformation pipelines
  - transformationList: # Array of transformation steps
```

### **5 Transformation Steps**

#### **Step 0: Synapse_Source (Extract)**
```yaml
transformationName: "Synapse_Source"
transformationType: "Source"
order: 0
overview: "Read from Synapse"
props:
  srcSysName: "Synapse"
  srcSysProp:
    sqlQueryFilePath: "/opt/nfs/edaikub/dg/prod/inbound/8999/diy/cust_risk_triggers/source.sql"
    server: "${SYNAPSE_SERVER:-synapse-server.database.windows.net}"
    database: "${SYNAPSE_DATABASE:-edw_prod}"
    username: "${SYNAPSE_USER:-sa}"
    password: "${SYNAPSE_PASSWORD}"
    port: 1433
    fallbackFormat: "csv"
    fallbackPath: "bank_data_cust_risk_triggers.csv"
```

#### **Step 1: expression1 (Transform)**
```yaml
transformationName: "expression1"
transformationType: "expression"
order: 1
overview: "Apply transformation to fields"
props:
  trnsfmRules: "customerLpid as customerLpid || custcardTokens as custcardTokens || cast(riskScore as decimal(10,2)) as riskScore || cast(riskTriggerDate as timestamp) as riskTriggerDate || riskIndicator as riskIndicator || cast(transactionAmount as decimal(15,2)) as transactionAmount || cast(transactionDate as date) as transactionDate || merchantCategory as merchantCategory"
inbound:
  src1: "Synapse_Source"
```

#### **Step 2: partitioner1 (Partition)**
```yaml
transformationName: "partitioner1"
transformationType: "partitioner"
order: 2
overview: "Repartition data for parallel processing"
props:
  partitionType: "REPARTITION"
  numPartition: "10"
inbound:
  src1: "expression1"
```

#### **Step 3: PostgreSQL-target (Load)**
```yaml
transformationName: "PostgreSQL-target"
transformationType: "Target"
order: 3
overview: "Persist customer risk triggers to PostgreSQL"
props:
  tgtSysName: "PostgreSQL"
  tgtSysProp:
    host: "${PG_HOST:-localhost}"
    port: "${PG_PORT:-5432}"
    database: "${PG_DATABASE:-bank_db}"
    username: "${PG_USER:-postgres}"
    password: "${PG_PASSWORD}"
    schema: "public"
    tableName: "cust_risk_triggers"
    writeMode: "append"
    batchSize: "102400"
    numPartitions: "10"
inbound:
  src1: "partitioner1"
```

#### **Step 4: RESTAPI-target (Push)**
```yaml
transformationName: "RESTAPI-target"
transformationType: "Target"
order: 4
overview: "Push risk triggers to REST API endpoint"
props:
  tgtSysName: "RESTAPI"
  tgtSysProp:
    request:
      uri: "https://digital-servicing-api.us.bank-dns.com/digital/servicing/risk-triggers"
      method: "POST"
      headers:
        - header-key: "Content-Type"
          header-value: "application/json"
        - header-key: "Correlation-ID"
          header-value: "risk-signal-trigger"
        - header-key: "Application-ID"
          header-value: "risk-trigger"
    authType: "MTLS"
    apiType: "rest"
    primaryKey: "customerLpid"
    rateLimit: 2000
    retryAttempts: 3
    retryDelayMs: 1000
inbound:
  src1: "PostgreSQL-target"
```

#### **Step 5: output_audit (Audit)**
```yaml
transformationName: "output_audit"
transformationType: "Target"
order: 5
overview: "Write to fixed width file"
props:
  tgtSysName: "fixedwidth-file"
  tgtSysProp:
    filePath: "/mnt/10152-cda-fileshare-1/10152/inbound/cams/auths/aged/processed/"
    fileRename:
      isRequired: true
      fileName: "cust_risk_triggers_audit.txt"
inbound:
  src1: "RESTAPI-target"
```

---

## 📊 Configuration Summary

| Component | Property | Value |
|-----------|----------|-------|
| **App Name** | appName | btot-8999-syn-api-cust-risk-tg |
| **Dataset** | dataSetName | cust-risk-triggers |
| **Source** | dataPipelineSource | ELZ |
| **Target** | dataPipelineTarget | api |
| **Deployment** | deploymentType | OnPrem |
| **Storage** | storageType | nfs |
| **System** | systemName | SYNAPSE |
| **Process** | processType | Batch |
| **Log Level** | logLevel | INFO |
| **Batch Size** | batchSize | 102400 |
| **Shuffle Partitions** | shufflePartition | 10 |

---

## ✅ File Location
`pipelines/configs/bank_cust_risk_triggers_config.yaml`

This configuration is now ready to use with the ETL pipeline matching your exact template format!

