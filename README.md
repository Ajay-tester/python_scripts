# **ETL Validation Framework**

This framework automates the validation of **ETL (Extract, Transform, Load)** processes. It provides a structured approach to define tests, execute them against your data pipelines, and generate comprehensive reports. This framework ensures data quality and integrity throughout your ETL workflow.

---

## **Features**

- **Automated Validation**: Define and execute validations on source and target data.
- **Dynamic Table Validation**: Automatically fetch schema details and validate tables dynamically.
- **Validation Types**:
  - Row count comparison
  - Duplicate record detection
  - Null value checks in mandatory columns
  - Accuracy validation between datasets
- **Comprehensive Logs**: Generates logs for each validation (e.g., row count mismatches, duplicates, null checks, etc.).
- **Modular Design**: Add new validations easily by extending the framework.

---

## **Installation**

### **Prerequisites**

- Python 3.7 or higher
- `pip` (Python package installer)
- Access credentials and connection details for your database
- Oracle Client configured (if using Oracle DB)

### **Setup Instructions**

1. Clone the repository to your local machine:

    ```bash
    git clone https://github.com/<your-repo>/etl-validation-framework.git
    cd etl-validation-framework
    ```

2. Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```

    Ensure the `requirements.txt` file includes all necessary libraries, such as:
    - `oracledb`
    - `pandas`
    - `pandasql`
    - `python-dotenv`

3. Configure your `.env` file with database connection details and paths:

    Example `.env` file:

    ```env
    ORACLE_CLIENT_PATH=/path/to/oracle_client
    DB_USERNAME=my_username
    DB_PASSWORD=my_password
    DB_DSN=my_database_dsn
    SOURCE_TABLE=HR.EMPLOYEES
    TARGET_TABLE=TARGET.TARGET_EMPLOYEES
    MANDATORY_COLUMNS_FILE=CSV_files/Not_null_columns.csv
    LOG_DIR=log_files
    ```

---

## **Usage**

Run the `main.py` file to execute validations. The script will log results to the specified log directory.

```bash
python main.py
