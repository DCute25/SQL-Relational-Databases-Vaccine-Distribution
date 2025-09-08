# Project Vaccine Distribution Overview

A PostgreSQL + Python project that simulates a vaccine distribution system.
It covers database initialization, data preprocessing, SQL queries, and analysis to generate insights into vaccination patterns, patient symptoms, and staff scheduling.

The project is divided into two main parts:

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
