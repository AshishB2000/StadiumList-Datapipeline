# Stadium List Data Pipeline

## About
This project is designed to automate the process of fetching, cleaning, and processing stadium data from Wikipedia using Python and Apache Airflow. The cleaned data is then stored in Azure Data Lake for further analysis and processing. This pipeline serves as a foundation for data-driven insights into stadium statistics and characteristics.

## Table of Contents
- [System Architecture](#system-architecture)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Running the Code With Docker](#running-the-code-with-docker)

## System Architecture
![System Architecture](Screenshot%202024-02-27%20101739.png)

The architecture outlines the data flow from source (Wikipedia) to destination (Azure Data Lake), detailing each step of the process including data scraping, cleaning, and transformation, managed and orchestrated by Apache Airflow.

## Requirements
- Python 3.9
- Docker
- PostgreSQL
- Apache Airflow 2.6

Ensure you have the above prerequisites installed and properly configured on your system before proceeding with the setup and execution of the data pipeline.

## Getting Started
To set up the project on your local machine, follow these steps:

1. **Clone the repository:**
```bash
git clone https://github.com/AshishB2000/StadiumList-Datapipeline
