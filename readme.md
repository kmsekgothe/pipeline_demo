## create venv
python -m venv venv
source venv/bin/activate

## install dependencies
pip install -r requirements.txt

## create databse
CREATE DATABASE go_db;

## create .env file
PGHOST=localhost
PGPORT=5432
PGDATABASE=go_db
PGUSER=
PGPASSWORD=

## from the project root , create the schema
python -m src.db.ddl

## run the full pipeline 
python -m src.main

This will:
Ingest source files defined in files_config.yaml
Transform and validate records
Load data into landing tables
Create analytical and data quality views
Log execution time for each stage