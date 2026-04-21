# Automated Multi-Source ETL Pipeline with Data Validation & Reporting

## Overview
An end-to-end ETL pipeline that ingests distributor data from Email (IMAP) and SFTP in multiple formats (CSV, Excel, TXT), performs schema normalization, data cleaning and validation against business rules and master data and loads validated records into final tables while generating automated error reports for invalid data.

## Features
- Multi-source ingestion (Email/IMAP + SFTP) supporting CSV, Excel and TXT
- Dynamic schema normalization across heterogeneous source files
- Data cleaning including trimming, datatype conversion, and deduplication using SQL window functions  
- Row-level validation: null checks, datatype mismatches, business rules and master data validation
- Data segregation into validated (final) and rejected (error) records with detailed messages
- Automated error reporting with mode-based routing (Email, SFTP)

## Pipeline Flow
Ingestion (Email/SFTP) → Raw Tables → Staging Table → Validation → Final & Error Tables → Reporting Layer (Email/SFTP)

## Architecture Diagram
<p align="center">
<img width="500" height="600" alt="image" src="https://github.com/user-attachments/assets/b4884069-483e-4b6e-bad9-94b3198e4ff1" />

## Tech Stack
- SQL Server (CTEs, window functions)
- SSIS (workflow orchestration)
- Python (imaplib, smtplib, pandas, openpyxl)
- Excel / CSV / TEXT handling

## Schema
- `raw_customer` - source-specific raw tables
- `stg_customer` - unified staging table with necessary columns
- `final_customer` - clean, validated records
- `error_table` - flagged records with detailed error messages
