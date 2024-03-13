# SerpApi
# Pipeline Documentation

## Overview

This pipeline automates the process of collecting reviews, processing them with GPT (for adding metadata such as categories and keywords), and finally storing the processed data into a Azure Tables. The process involves several components, including an AWS EC2 instance, Azure Blob Storage, and two trigger-based mechanism for data processing and storage.

## Components

1. **Collector (`App.py`)**: An application running on AWS EC2 that collects reviews from various sources (e.g., Google Maps via SerpAPI) and stores them in an Azure Blob Storage container named `reviews`.

2. **Processor (`function_app.py`)**: An Azure Function triggered by the arrival of new data in the `reviews` blob. It processes the data by using GPT to add additional columns (`cat` and `keywords`) and stores the enriched data in another blob container named `gpt-reviews`.

3. **Preprocessor and Azure Table Inserter (`function_app.py`)**: Another Azure Function that triggers on data arrival in `gpt-reviews`. It preprocesses the file further if necessary and inserts the final data into a database table for permanent storage and query support.

## Setup and Execution

### AWS EC2 Setup

1. Deployed `App.py` to an AWS EC2 instance.
2. Ensure Python 3.x and necessary libraries (`pandas`, `azure-storage-blob`, `serpapi`, etc.) are installed.
3. Configure the SerpAPI key and Azure Blob Storage credentials in the script.

### Azure Blob Storage Configuration

1. Create two blob containers: `reviews` and `gpt-reviews`.
2. Set up necessary permissions and retrieve connection strings for use in the scripts.

### Azure Function App

1. Deploy `function_app.py` to an Azure Function Apps.
2. Configure the Function App to trigger on blob creation events in `reviews` and `gpt-reviews` blobs.
3. Ensure the Function App has access to Azure Blob Storage and any other necessary resources (e.g., Azure Table Storage for intermediate data storage).

## Additional Notes

- Ensure all API keys and sensitive credentials are stored securely and not hard-coded in the scripts.
- The pipeline requires configuration of Azure Blob Storage triggers for automation.
- Monitor the Azure Function App logs for errors or issues during processing.


![image](https://github.com/Zeeshan-Muhimma/SerpApi/assets/146631977/e76bcb4c-7364-4773-90a9-8bdab9b618aa)
