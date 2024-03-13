import azure.functions as func
import logging
import pandas as pd
import openai
# from openai import OpenAI
import time
from io import BytesIO
import io
from azure.storage.blob import BlobServiceClient, BlobClient
from datetime import datetime

openai.api_key = ''    
category_to_keyword = {
    'convenience': ['location'],
    'service': ['service', 'staff'],
    'environment': ['cleanliness'],
    'experience': ['service'],
    'quality': ['food quality'],
    'value': ['food quality']
}
def find_relevant_category(review):
    # Define the category descriptions
    # client = OpenAI()
    category_descriptions = {
        'Convenience': 'Assess the ease of location access, parking availability, and transactional efficiency.',
        'Service': 'Evaluate staff interaction, responsiveness, and overall customer service quality.',
        'Environment': 'Focus on cleanliness, safety, and the physical and aesthetic ambiance of the establishment.',
        'Experience': 'Consider the overall customer satisfaction, including atmosphere and service interaction.',
        'Quality': 'Look at the product or menu item variety, uniqueness, and overall quality.',
        'Value': 'Judge the pricing, affordability, and cost-benefit ratio of products or services.'
    }
    review_str = str(review).strip() if review is not None and not pd.isna(review) else ''
    if review_str == '':
        print("Review is NaN or empty, skipping API request.")
        return None, []
    review = review.strip()
    print(review)
    prompt = f"Given the following review: {review}, identify the most relevant category from these options: {', '.join(category_descriptions.keys())}. Please respond with the category name only."

    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",  # Use the chat model
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt}
            ]
            
        )
        # Extract the category from the response
        category = response['choices'][0]['message']['content'].strip()
        
        # Convert category to lowercase for dictionary key matching
        category_key = category.lower()

        # Now map the extracted category to your keywords.
        relevant_keywords = category_to_keyword.get(category_key, [])
        print(f"Identified category: {category}, Relevant keywords: {relevant_keywords}")
        return category, relevant_keywords

    except openai.error.OpenAIError as e:
        print(f"OpenAI API error: {str(e)}")
        return "Error", []
    except Exception as e:
        print(f"Other error occurred: {str(e)}")
        return "Error", []

def save_reviews_to_excel_blob(reviews_df):
    try:
        # Define your Blob Storage connection string
        blob_connection_string = ''
        
        # Create a BlobServiceClient using the connection string
        blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
        
        # Get the current date and time
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        # Define the file name using the current date
        file_name = f"GPT_reviews_{current_date}.xlsx"
        
        # Convert the DataFrame to Excel format
        excel_data = io.BytesIO()
        with pd.ExcelWriter(excel_data, engine='xlsxwriter') as writer:
            reviews_df.to_excel(writer, index=False)
        excel_data.seek(0)
        
        # Get the BlobClient for the new blob
        blob_client = blob_service_client.get_blob_client(container="gpt-reviews", blob=file_name)
        
        # Upload the Excel data to the blob
        blob_client.upload_blob(excel_data, overwrite=True)
        
        return file_name
    except Exception as e:
        logging.error(f"Error saving reviews to Blob Storage: {str(e)}")
        return None



app = func.FunctionApp()
@app.blob_trigger(arg_name="myblob", path="reviews/{name}",
                               connection="muhimmablob_STORAGE") 
def blob_trigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    blob_bytes = myblob.read()
    blob_stream = BytesIO(blob_bytes)
    blob_stream.seek(0) 
        # Read the blob content into a DataFrame
    df = pd.read_excel(blob_stream, index_col=None, header=0,engine='openpyxl') 
    df['category'] = ""
    df['keywords'] = ""
    for index, row in df.iterrows():
        category, keywords = find_relevant_category(row['snippet'])
        df.at[index, 'category'] = category
        df.at[index, 'keywords'] = ', '.join(keywords)    
    excel_file_name = save_reviews_to_excel_blob(df)
    if excel_file_name:
        # Log the file name
        logging.info(f"Reviews saved to Excel file in Blob Storage: {excel_file_name}")
    else:
        logging.error("Error saving reviews to Blob Storage.")
                
    logging.info(f"Python blob trigger function processed blob")



