from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
import os
import io
import pandas as pd
from dotenv import load_dotenv
import duckdb
import pyarrow as pa
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

def webscrape_data(url):

    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(options=options)

    driver.get(url)
    actions = ActionChains(driver)

    posts = driver.find_elements(By.CLASS_NAME, "eael-grid-post-holder-inner")

    results = []

    for post in posts:

        # Categories (hidden but in DOM)
        category_elements = post.find_elements(
            By.CSS_SELECTOR, ".post-carousel-categories a"
        )

        categories = [
            c.get_attribute("textContent").strip()
            for c in category_elements
            if c.get_attribute("textContent").strip()
        ]

        # Title + URL
        title_element = post.find_element(
            By.CSS_SELECTOR, ".eael-entry-title a"
        )

        title = title_element.get_attribute("textContent").strip()
        url = title_element.get_attribute("href")
        title_attr = title_element.get_attribute("title")

        # Author
        author_element = post.find_element(
            By.CSS_SELECTOR, ".eael-posted-by a[rel='author']"
        )

        author = author_element.get_attribute("textContent").strip()
        author_url = author_element.get_attribute("href")

        results.append({
            "categories": categories,
            "title": title,
            "url": url,
            "title_attribute": title_attr,
            "author": author,
            "author_url": author_url
        })
    df = pd.DataFrame(results)
    return df


def historic_data(MD_TOKEN):
    con = duckdb.connect('md:?motherduck_token=' + MD_TOKEN)
    his_df = con.sql("SELECT * FROM wow_data.wow_historic_data").df()
    con.close()
    his_df['Year_Week'] = his_df['Year'].astype(str) +  his_df['Week'].astype(str).str.zfill(2)
    his_df['Year_Week'] = his_df['Year_Week'].astype(int)
    return his_df


def transform_webscrape_data(webscraped_data_df):
    #remove the brackets and quotes in the categories and strip whitespace. If Category is empty, replace with "Other"
    webscraped_data_df['Category'] = (
        webscraped_data_df['categories']
        .astype(str)
        .str.replace(r"[\[\]']", "", regex=True)
        .str.strip()
    )
    webscraped_data_df['Category'] = webscraped_data_df['Category'].replace('', "Other").str.title()

    #Extract the year and week number from the url column and convert it to int
    webscraped_data_df['Year'] = webscraped_data_df['url'].str.extract(r'.*(\d{4}).*').astype(int)
    webscraped_data_df['Week'] = webscraped_data_df['url'].str.extract(r'.*w(\d+)[tab|\/].*').astype(int)

    #Create a Year_Week column by concatenating the Year and Week columns, with week number zero-padded to 2 digits
    webscraped_data_df['Year_Week'] = webscraped_data_df['Year'].astype(str) + webscraped_data_df['Week'].astype(str).str.zfill(2)
    webscraped_data_df['Year_Week'] = webscraped_data_df['Year_Week'].astype(int)

    column_rename_mapping = {
    'title': 'Title',
    'url': 'Link',
    'author': 'Author'}
    webscraped_data_df.rename(columns=column_rename_mapping, inplace=True)

    #Drop some columns to match with the historical data
    webscraped_data_df.drop(columns=['categories','author_url','title_attribute'], inplace=True)
    #Order columns to match with the historical data
    webscraped_data_df = webscraped_data_df[['Category', 'Link', 'Title', 'Author', 'Year', 'Week', 'Year_Week']]
    return webscraped_data_df

def load_incremental_models_to_warehouse(MD_TOKEN,historical_data_df, webscraped_data_df):
    max_year_week = historical_data_df['Year_Week'].max()
    webscraped_data_df = webscraped_data_df[webscraped_data_df['Year_Week'] > max_year_week]
    webscraped_data_df = webscraped_data_df.drop(columns=['Year_Week']).reset_index(drop=True)
    with duckdb.connect('md:?motherduck_token=' + MD_TOKEN) as conn:
        arrow_table = pa.table(webscraped_data_df)
        conn.sql("""
                            INSERT INTO wow_data.wow_historic_data
                            SELECT * FROM arrow_table
                            """)
        print("New Data Successfully inserted into Datawarehouse")

def return_data_from_warehouse(MD_TOKEN):
    con = duckdb.connect('md:?motherduck_token=' + MD_TOKEN)
    df = con.sql("SELECT * FROM wow_data.wow_historic_data ORDER BY Year DESC, Week DESC").df()
    con.close()
    return df

def get_drive_service(GD_TOKEN_FILE, SCOPES):
    creds = None
    if os.path.exists(GD_TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(GD_TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            raise Exception(
                "token.json not found or invalid. Run OAuth locally first."
            )
    
    service = build("drive", "v3", credentials=creds)
    return service

def upload_to_google_drive(gd_service, GD_FOLDER_ID, df, file_name):
    service = gd_service

    #Save dataframe to in-memory CSV
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    media = MediaIoBaseUpload(
        io.BytesIO(csv_buffer.getvalue().encode()),
        mimetype="text/csv"
    )

    file_metadata = {
        "name": file_name,
        "parents": [GD_FOLDER_ID]
    }

    uploaded_file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id, name, webViewLink",
        supportsAllDrives=True
    ).execute()

    print(f"Successfully uploaded '{file_name}' with ID: {uploaded_file['id']}")
    print(f"View file: {uploaded_file.get('webViewLink', 'No link available')}")
    return uploaded_file.get("id")


def main():
    load_dotenv()
    MD_TOKEN = os.getenv("MD_TOKEN")
    url = "https://www.workout-wednesday.com/latest/"

    print("===================================================")
    historical_data_df = historic_data(MD_TOKEN)
    print("Historic Data:")
    print(historical_data_df.head())

    print("===================================================")
    print("Webscraped Data:")
    webscraped_data_df = webscrape_data(url)
    #========================================================================


    webscraped_data_df = transform_webscrape_data(webscraped_data_df)

    # print("historical data here: ")
    # print(historical_data_df.head())

    print("===================================================")
    print("webscraped data after transformation here: ")
    print(webscraped_data_df.head())

    print("Incremental models. Load data into warehouse:")
    load_incremental_models_to_warehouse(MD_TOKEN, historical_data_df, webscraped_data_df)
    

    data_from_wh_df = return_data_from_warehouse(MD_TOKEN)
    print("Data from warehouse:")
    print(data_from_wh_df.head())

    #===============================================================
    GD_TOKEN_FILE = "token.json"
    SCOPES = ["https://www.googleapis.com/auth/drive.file"]
    GD_FOLDER_ID = os.getenv("GD_FOLDER_ID")

    gd_service = get_drive_service(GD_TOKEN_FILE, SCOPES)

    upload_to_google_drive(gd_service, GD_FOLDER_ID, data_from_wh_df, "wow_historic_data.csv")


if __name__ == "__main__":
    main()