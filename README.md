# **Sales Data Pipeline Optimization & Automation**

## **Project Overview**
NeoCommerce Inc. is a mid-sized e-commerce company that requires an optimized and automated ETL pipeline for sales analytics. The current data ingestion process is inefficient, leading to delays in reporting and inconsistent data quality. This project aims to streamline data ingestion, transformation, and loading into a BigQuery data warehouse.

## **Project Scope**
The project involves:
1. **Extracting data** from multiple sources:
   - Google Cloud SQL (Order Management System)
   - REST API (Product Information Service)
   - CSV files stored in Google Cloud Storage (Customer Feedback)
2. **Loading raw data** into BigQuery.
3. **Transforming data** within BigQuery using scheduled queries.
4. **Automating workflows** using Apache Airflow.
5. **Implementing monitoring & logging** for failure detection and alerts.

## **Tech Stack**
- **Data Sources:** Google Cloud SQL, REST API, CSV files (Google Cloud Storage)
- **ETL Pipeline:** Python (Pandas, SQLAlchemy), Apache Airflow
- **Data Storage:** BigQuery
- **Cloud Services:** Google Cloud Platform (GCP)
- **Monitoring & Logging:** Prefect/Airflow logs, Slack alerts

## **Source Data Details & Transformations**

### **1. Google Cloud SQL (Order Management System)**
#### **Schema Overview:**
- `orders`: Contains order details.
- `order_items`: Stores products per order.
- `customers`: Holds customer information.
- `payments`: Tracks payments.

#### **Required Checks & Transformations:**
✅ Ensure `order_id`, `customer_id`, and `product_id` are **not NULL**.
✅ Validate `order_date` format (**YYYY-MM-DD HH:MM:SS**).
✅ Standardize `status` values (**e.g., "shipped" → "SHIPPED"**).
✅ Convert `order_date` to **UTC timezone**.
✅ Load raw data into BigQuery.

---

### **2. REST API (Product Information Service)**
#### **Example API Response:**
```json
{
  "product_id": 1001,
  "name": "Wireless Headphones",
  "category": "Electronics",
  "price": 125.25,
  "in_stock": true
}
```
#### **Required Checks & Transformations:**
✅ Ensure `product_id` exists in `order_items` (data consistency check).
✅ Validate `price` is numeric and non-negative.
✅ Convert `in_stock` values (`true/false`) to `1/0`.
✅ Load raw product data into BigQuery.

---

### **3. CSV Files (Customer Feedback – Stored in Google Cloud Storage)**
#### **Schema Overview:**
- `order_id`: Order reference.
- `customer_id`: Customer reference.
- `feedback_text`: Text feedback.
- `rating`: 1-5 rating.
- `feedback_date`: Date of feedback.

#### **Required Checks & Transformations:**
✅ Ensure **no duplicate feedback** for the same order.
✅ Validate `rating` is between **1-5**.
✅ Convert `feedback_date` to `YYYY-MM-DD` format.
✅ Remove special characters/emojis from `feedback_text`.
✅ Load raw feedback data into BigQuery.

---

## **Data Pipeline Workflow**
1. **Extract & Load**
   - Retrieve MySQL order data from Google Cloud SQL.
   - Fetch product details from the API.
   - Load customer feedback from Google Cloud Storage.
   - Store all raw data into **BigQuery staging tables**.
2. **Transform** (via **BigQuery Scheduled Queries**)
   - Apply data quality checks and transformations.
   - Join datasets into a **single fact table**.
3. **Final Data Storage**
   - Store transformed data in **BigQuery fact tables**.

### **Final Data Structure (Fact Table in BigQuery)**
| order_id | customer_id | order_date | total_amount | status | product_name | category | price | in_stock | rating | feedback_text |
|----------|------------|------------|--------------|--------|--------------|----------|-------|---------|--------|---------------|
| 10001    | 501        | 2024-01-01 14:23:00 | 250.50 | SHIPPED | Wireless Headphones | electronics | 125.25 | 1 | 5 | "Great product, fast shipping" |

---

## **Workflow Automation with Airflow**
✅ **Airflow DAG** automates data extraction and loading (EL).
✅ **Incremental Load**: Uses `last_updated` timestamps to fetch only new records.
✅ **Error Handling & Logging**: Logs API failures and corrupt CSV records.
✅ **Monitoring & Alerts**: Sends Slack/email alerts for pipeline failures.
✅ **BigQuery Scheduled Queries** handle transformation and final table creation.

---

## **Project Deliverables**
✔ Fully functional **Airflow DAG** for ingestion.
✔ SQL scripts for **BigQuery scheduled queries & transformations**.
✔ Well-structured and commented **code repository (GitHub/Bitbucket)**.
✔ A detailed **README file** explaining setup and execution.

---

## **Setup & Execution**
### **1. Environment Setup**
```bash
# Clone the repository
git clone https://github.com/your-repo/sales-data-pipeline.git
cd sales-data-pipeline

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### **2. Airflow DAG Setup**
```bash
# Start Airflow services
airflow standalone

# Open Airflow UI (default: http://localhost:8080)
```

### **3. Running the Pipeline**
```bash
# Trigger DAG manually
airflow dags trigger sales_pipeline
```

### **4. BigQuery Scheduled Query Setup**
1. Navigate to **BigQuery Console**.
2. Create a **Scheduled Query** for transforming raw data.
3. Ensure the query executes at the required frequency (e.g., hourly/daily).
4. Verify that transformed data is correctly stored in the **fact table**.


