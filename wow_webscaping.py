from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
import os
import pandas as pd
from dotenv import load_dotenv
import duckdb

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
    return his_df

def main():
    load_dotenv()
    MD_TOKEN = os.getenv("MD_TOKEN")
    url = "https://www.workout-wednesday.com/latest/"

    # print("===================================================")
    # historic_data_df = historic_data(MD_TOKEN)
    # print("Historic Data:")
    # print(historic_data_df.head())

    # print("===================================================")
    # print("Webscraped Data:")
    # webscraped_df = webscrape_data(url)

    #========================================================================
    # Transforming data by using the local files

    historical_data_df = pd.read_csv("wow_historic_data.csv")
    webscraped_data_df = pd.read_csv("webscraped_data_local.csv")

    print("historical data here: ")
    print(historical_data_df.head())

    print("===================================================")
    print("webscraped data here: ")
    print(webscraped_data_df.head())

if __name__ == "__main__":
    main()