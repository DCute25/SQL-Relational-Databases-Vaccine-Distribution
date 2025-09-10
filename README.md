# SQL Project: Vaccine Distribution Relational Database

This project implements a vaccine distribution management system using **PostgreSQL** and **Python**.

The workflow begins with designing and creating a **relational database schema in SQL**, defining tables with primary and foreign keys to represent patients, vaccines, staff, and appointments. After establishing the database structure, data is imported and cleaned from Excel files, ensuring consistency and validity. Once the database is populated, **Python scripts are used to run SQL queries**, analyze vaccination trends, and generate visual insights into vaccine distribution.

It covers **database initialization, schema creation, data preprocessing, query execution, and analysis** to generate insights into vaccination patterns, patient symptoms, and staff scheduling. The project is divided into two main parts:

## Database Initialization

    Reads and preprocesses Excel data
    
    Cleans invalid values (e.g., dates, missing entries)
    
    Renames columns for consistency
    
    Writes structured data into PostgreSQL tables while handling foreign key constraints

## Data Analysis and Queries

    Executes SQL queries to extract and join vaccination data
    
    Analyzes patient symptoms, vaccination status, and age groups
    
    Evaluates vaccine usage and attendance rates
    
    Creates plots and summary tables for deeper insights

## Features

    PostgreSQL integration using psycopg2 and SQLAlchemy
    
    Data cleaning and preprocessing with pandas and numpy
    
    SQL query execution with joins, aggregations, and transformations

## Exploratory data analysis including:

    Patient symptoms by gender
    
    Vaccination progress over time
    
    Vaccination status distribution by age group
    
    Vaccine attendance and frequency analysis
    
    Visualization of vaccination trends with matplotlib

## Technologies Used

    Python 3.x
    
    PostgreSQL
    
    Pandas
    
    NumPy
    
    SQLAlchemy
    
    Psycopg2
    
    Matplotlib

## Project Structure
project-root/
│

├── data/

│   └── vaccine-distribution-data.xlsx     # Raw data (Excel)

│

├── code/

│   ├── postgres_init.py                   # Database initialization & preprocessing

│   └── analysis_queries.py                # SQL queries & data analysis

│

├── database/

│   └── db_schema.sql                      # Database schema definitions

│

├── q9-vaccines-plot.png                   # Example output plot

└── README.md                              # Project documentation
