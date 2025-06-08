# Customer-and-Sales-Data-Pipeline-using-AWS-PySpark-MySQL

# Customer Sales Analysis Data Pipeline

This project implements a data engineering pipeline for customer sales analysis using AWS S3, MySQL, and Apache Spark. It automates the ingestion, validation, transformation, and loading of sales data, supporting robust data mart creation and error handling.

## Project Structure

```
.
├── docs/
│   ├── architecture
│   ├── architecture.png
│   ├── database_schema.drawio.png
│   └── readme.md
├── resources/
│   ├── dev/
│   │   ├── config.py
│   │   └── requirements.txt
│   ├── prod/
│   │   ├── config.py
│   │   └── requirements.txt
│   ├── qa/
│   │   ├── config.py
│   │   └── requirements.txt
│   ├── sql_scripts/
│   │   └── table_scripts.sql
│   └── __init__.py
├── src/
│   ├── main/
│   │   ├── delete/
│   │   ├── download/
│   │   ├── move/
│   │   ├── read/
│   │   ├── transformations/
│   │   │   └── jobs/
│   │   ├── upload/
│   │   ├── utility/
│   │   ├── write/
│   │   └── __init__.py
│   ├── test/
│   └── __init__.py
```

## Features

- **AWS S3 Integration:** Upload, download, move, and delete files in S3 buckets.
- **MySQL Integration:** Read and write data to MySQL tables, including data marts and staging tables.
- **Data Validation:** Validates schema and required columns for incoming CSV files.
- **Error Handling:** Moves invalid files to error directories and logs issues.
- **Data Transformation:** Uses Apache Spark for ETL and data mart calculations.
- **Configurable Environments:** Separate configs for dev, qa, and prod.

## Setup

### Prerequisites

- Python
- [MySQL]
- [AWS Account]
- [Java] (for Spark)
- [Apache Spark]
- AWS credentials with S3 access

### Installation

1. **Clone the repository:**
   ```sh
   git clone https://github.com/yourusername/customer-sales-analysis.git
   cd customer-sales-analysis
   ```

2. **Set up a virtual environment:**
   ```sh
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```sh
   pip install -r resources/dev/requirements.txt
   ```

4. **Configure AWS and MySQL:**
   - Edit `resources/dev/config.py` with your AWS keys and MySQL credentials.
   - Encrypt your AWS keys as required by the project.

5. **Set up the database:**
   - Use the SQL scripts in `resources/sql_scripts/table_scripts.sql` to create the necessary tables.

## Usage

1. **Run the main pipeline:**
   - Open your IDE or terminal.
   - Activate your virtual environment.
   - Run the main job:
     ```sh
     python src/main/transformations/jobs/main.py
     ```

2. **Data Flow:**
   - The pipeline will:
     - List and download new sales data from S3.
     - Validate and process CSV files.
     - Move invalid files to error directories.
     - Load valid data into MySQL staging and data mart tables.
     - Clean up local and S3 processed files.

## Project Architecture

See [`docs/architecture.png`](docs/architecture.png) for a high-level overview.

## Database Schema

See [`docs/database_schema.drawio.png`](docs/database_schema.drawio.png) for the ER diagram.

## Troubleshooting

- Check logs for errors in the output or log files.
- Ensure AWS credentials and MySQL access are correct.
- For Spark issues, verify your Java and Spark installations.
