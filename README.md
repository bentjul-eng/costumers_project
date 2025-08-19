# Complete ETL process

This repository contains the solution developed for demonstrate skills in **data engineering**, including **synthetic data generation**, **processing with Apache Spark**, and **software engineering best practices**.

---

## 📂 Project Structure

```bash
.
├── data/                     # Generated datasets (DS1, DS2, DS3)
├── src/                      # Main source code
│   ├── data_generation.py    # Script to generate datasets
│   ├── data_queries.py       # Script with Spark queries
│   ├── json_export.py        # Script to generate JSON files per customer
│   └── utils/                # Helper functions and utilities
├── tests/                    # Unit tests
├── my_experience.md          # Notes about development process
├── requirements.txt          # Project dependencies
└── README.md                 # This file


## 🚀 Technologies Used

* **Python 3.9+**
* **Apache Spark (PySpark)**
* **Faker** for synthetic data generation
* **Pytest** for unit testing
* **Docker** *(optional, if containerization is implemented)*

## ⚙️ Setup & Execution

### 1. Clone the repository

```bash
git clone https://github.com/bentjul-eng/costumers_project.git
cd costumers_project
```

### 2. Create and activate a virtual environment

```bash
python -m venv venv
source venv/bin/activate   # Linux/Mac
venv\Scripts\activate      # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Generate synthetic data

```bash
python src/data_generation.py
```

This will create **CUSTOMER_DATA (DS1)**, **CREDITCARD_TRANSACTION (DS2)** and **PRODUCT_TRANSACTION (DS3)** inside the `data/` folder.

### 5. Run Spark queries

```bash
python src/data_queries.py
```

Produces the required dataframes:
* **Top 100 customers by single biggest transaction**
* **Summary by credit card provider (transactions between 00:00–12:00)**

### 6. Export JSONs for top 100 customers

```bash
python src/json_export.py
```

Creates one JSON file per customer containing:
* All customer details
* Their biggest transaction
* All product details from that transaction

## 🧪 Running Tests

To execute unit tests:

```bash
pytest tests/
```

## 📊 Expected Results

### Query 1
Top 100 customers by largest single transaction:
* CUSTOMER_ID
* TRANSACTION_ID
* TRANSACTION_VALUE
* TRANSACTION_DATE_TIME

### Query 2
Summary by credit card provider (00:00–12:00):
* CREDITCARD.PROVIDER
* Total quantity of items
* Total value of items

### JSON Files
One file per customer, containing:
* Full customer details
* Their largest transaction details
* All product details for that transaction

## 🏆 Bonus (optional)

* **Containerization**: run both data generation and queries within a Docker container.
* **Cloud Architecture**: design a cloud architecture diagram (e.g., S3, EMR, Glue, Kafka, etc.).
* **Streaming with Kafka + Spark Structured Streaming**: ingest and persist transactions in real-time.

## 📌 Notes

* All processing is done using the **Spark DataFrame API** (no Spark SQL).
* Code is structured with separation of concerns for **generation**, **querying**, and **export**.
* Dependencies are centralized in `requirements.txt`.


## ✍️ Author

**Julia Bento**.
