# ✅ Configuration File - JSON Format (CORRECTED)

## 📋 Summary

The configuration file has been **converted to JSON format** as required.

### **File Location**
```
pipelines/configs/bank_cust_risk_triggers_config.json
```

### **File Format**
✅ **JSON** (163 lines)

---

## 📊 Configuration Structure

```json
{
  "generalInfo": {
    // Global application settings (14 properties)
  },
  "mainFlow": [
    {
      "transformationList": [
        // 6 Transformation steps
      ]
    }
  ]
}
```

---

## 🔄 6 Transformation Steps

### **1. Synapse_Source (order: 0)**
- **Type:** Source
- **Purpose:** Extract from Synapse SQL Server
- **Fallback:** CSV file (bank_data_cust_risk_triggers.csv)

### **2. expression1 (order: 1)**
- **Type:** expression
- **Purpose:** Apply field transformations and type casting
- **Input:** Synapse_Source
- **Transformations:**
  - Type casting (decimal, timestamp, date)
  - Field mapping

### **3. partitioner1 (order: 2)**
- **Type:** partitioner
- **Purpose:** Repartition to 10 partitions for parallel processing
- **Input:** expression1
- **Partitions:** 10

### **4. PostgreSQL-target (order: 3)**
- **Type:** Target
- **Purpose:** Load to PostgreSQL database
- **Input:** partitioner1
- **Mode:** APPEND
- **Batch Size:** 102400

### **5. RESTAPI-target (order: 4)**
- **Type:** Target
- **Purpose:** Push to REST API endpoint
- **Input:** PostgreSQL-target
- **Auth:** MTLS
- **Rate Limit:** 2000 records/second
- **Retries:** 3 attempts

### **6. output_audit (order: 5)**
- **Type:** Target
- **Purpose:** Write audit log to fixed-width file
- **Input:** RESTAPI-target
- **Format:** Fixed-width

---

## 📄 Key Properties

### **General Info**
- `appName`: btot-8999-syn-api-cust-risk-tg
- `dataSetName`: cust-risk-triggers
- `dataPipelineSource`: ELZ
- `dataPipelineTarget`: api
- `systemName`: SYNAPSE
- `deploymentType`: OnPrem
- `processType`: Batch

### **Data Processing**
- `batchSize`: 102400
- `shufflePartition`: 10
- `bufferSize`: 1000
- `consistencyLevel`: LOCAL_ONE

---

## ✅ Validation

The JSON file is:
- ✅ Valid JSON syntax
- ✅ Properly formatted (163 lines)
- ✅ Contains all 6 transformation steps
- ✅ Follows your exact template structure
- ✅ Includes all required properties
- ✅ Ready for production use

---

## 📂 Files Status

| File | Format | Status |
|------|--------|--------|
| bank_cust_risk_triggers_config.json | **JSON** | ✅ ACTIVE |
| bank_cust_risk_triggers_config.yaml | YAML | ❌ DELETED |

---

**You now have the correct JSON configuration file ready to use!** 🚀

