import logging
from azure.data.tables import TableServiceClient
import os
import pandas as pd
from serpapi import GoogleSearch
from azure.storage.blob import BlobServiceClient, BlobClient
import io
from datetime import datetime, timedelta
import re
from dateutil.relativedelta import relativedelta

def fetch_reviews_for_identifier(data_id):
    params = {
        "api_key": "",  # Use your actual API key
        "engine": "google_maps_reviews",
        "data_id": data_id,
        "hl": "en",
        "sort_by": "newestFirst"
    }
    params2 = {
        "api_key": "",  # Use your actual API key
        "engine": "google_maps_reviews",
        "data_id": data_id,
        "hl": "ar",
        "sort_by": "newestFirst"
    }
    search1 = GoogleSearch(params)
    search2 = GoogleSearch(params2)

    try:
        results1 = search1.get_dict()
        results2 = search2.get_dict()
        reviews1 = results1.get("reviews", [])  # Use .get to avoid KeyError
        reviews2 = results2.get("reviews", [])  # Use .get to avoid KeyError
    except Exception as e:
        print(f"Error fetching reviews for {data_id}: {e}")
        reviews = []  # Return an empty list in case of error
    # Merge the two lists of reviews
    reviews = reviews1 + reviews2
    return data_id, reviews

def fetch_identifiers():
    connection_string = ''
    table_service = TableServiceClient.from_connection_string(conn_str=connection_string)
    table_client = table_service.get_table_client(table_name="StoreInfo")
    
    entities = table_client.list_entities()
    
    identifiers = [entity['Identifier'] for entity in entities]
    return identifiers

def save_reviews_to_excel_blob(reviews_df):
    try:
        blob_connection_string = ''
        blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
        
        current_date = datetime.now().strftime("%Y-%m-%d")
        file_name = f"reviews_{current_date}.xlsx"
        
        excel_data = io.BytesIO()
        with pd.ExcelWriter(excel_data, engine='xlsxwriter') as writer:
            reviews_df.to_excel(writer, index=False)
        excel_data.seek(0)
        
        blob_client = blob_service_client.get_blob_client(container="reviews", blob=file_name)
        
        blob_client.upload_blob(excel_data, overwrite=True)
        
        return file_name
    except Exception as e:
        logging.error(f"Error saving reviews to Blob Storage: {str(e)}")
        return None

def convert_relative_date_to_absolute(date_str, current_date):
    if 'قبل' in date_str:
        num = re.search(r'\d+', date_str)
        if 'دقيقة' in date_str or 'دقائق' in date_str:
            unit = 'minute'
        elif 'ساعة' in date_str or 'ساعات' in date_str:
            unit = 'hour'
        elif 'يوم' in date_str or 'أيام' in date_str:
            unit = 'day'
        elif 'أسبوع' in date_str or 'أسابيع' in date_str:
            unit = 'week'
        elif 'شهر' in date_str:
            unit = 'month'
        else:
            return None  # Unrecognized unit
        num = int(num.group(0)) if num else 1  # Default to 1 if not explicitly mentioned
    elif 'ago' in date_str:
        if 'minute' in date_str:
            unit = 'minute'
        elif 'hour' in date_str:
            unit = 'hour'
        elif 'day' in date_str:
            unit = 'day'
        elif 'week' in date_str:
            unit = 'week'
        elif 'month' in date_str:
            unit = 'month'
        else:
            return None  # Unrecognized unit
        num = re.search(r'\d+', date_str)
        num = int(num.group(0)) if num else 1  # Default to 1 if not explicitly mentioned
    else:
        return None  # Not a relative date string
    
    if unit == 'minute':
        return current_date - timedelta(minutes=num)
    elif unit == 'hour':
        return current_date - timedelta(hours=num)
    elif unit == 'day':
        return current_date - timedelta(days=num)
    elif unit == 'week':
        return current_date - timedelta(weeks=num)
    elif unit == 'month':
        return current_date - relativedelta(months=num)
    
    return None
        
def determine_language(snippet):
    if pd.isna(snippet):
        return None
    arabic_count = sum('\u0600' <= c <= '\u06FF' for c in snippet)
    english_count = sum('a' <= c.lower() <= 'z' for c in snippet)
    
    if arabic_count > english_count:
        return 'ar'
    else:
        return 'eng'

def main():
    logging.basicConfig(level=logging.INFO)
    
    logging.info("Fetching identifiers...")
    identifiers = fetch_identifiers()
    
    all_reviews = []
    for identifier in identifiers:
        logging.info(f"Fetching reviews for identifier: {identifier}")
        _, reviews = fetch_reviews_for_identifier(identifier)
        for review in reviews:
            review['Identifier'] = identifier  # Correct field name based on your provided code snippet
        all_reviews.extend(reviews)
    
    if not all_reviews:
        logging.info("No reviews fetched.")
        return

    logging.info("Processing reviews...")
    reviews_df = pd.json_normalize(all_reviews)
    current_date = datetime.now()
    reviews_df['Date'] = reviews_df['date'].apply(lambda x: convert_relative_date_to_absolute(x, current_date))
    reviews_df['Language'] = reviews_df['snippet'].apply(determine_language)

    logging.info("Saving reviews to Excel in blob storage...")
    excel_file_name = save_reviews_to_excel_blob(reviews_df)
    if excel_file_name:
        logging.info(f"Reviews saved to Excel file in Blob Storage: {excel_file_name}")
    else:
        logging.error("Failed to save reviews to Excel.")

if __name__ == "__main__":
    main()