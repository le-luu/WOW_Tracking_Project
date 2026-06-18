# Workout Wednesday Challenges Tracker

## Overview

This project tracks and analyzes Workout Wednesday (WOW) challenges for Tableau and Power BI by building an automated ETL pipeline.

The project was created as a hands-on Data Engineering exercise to practice:

- Data extraction from a dynamic webpage
- Data transformation and cleaning
- Incremental data model
- Dashboard development
- CI/CD and workflow automation

The pipeline automatically collects the new published Workout Wednesday challenges and updates the reporting dashboard without manual intervention.

<img src="https://github.com/le-luu/WOW_Tracking_Project/blob/main/img/pipeline.png"  />

## Architecture

The whole data pipeline consists of five stages:

### 1. Extract

The historical data was extract from a static WOW page before. Then, the dashboard was built based on that historical data. Later, the WOW page was updated to a dynamic webpage. To extract the data from a dynamic webpage, the project used Selenium package in Python to webscape both Power BI and Tableau challenges. In the stage, it will collect:

- Challenge Categories
- Title
- Posted Date
- Challenge url
- Author
- Author url

The historical data was extracted from the Tableau Dashboard before. Then, it was uploaded into a data warehouse (MotherDuck). In the project, pulling the historical data to help deciding the latest challenge on the warehouse. Later, it will be helpful for the incremental model in loading.

### 2. Transform

In this stage, the data after extracting from the webpage will be cleaned and transformed:

- Extract week, year number using REGEX
- Rename columns
- Remove unnecessary columns
- Ensure that the schema and data type of each column matched with the schema of the historical data on the data warehouse

**Incremental Processing**
To avoid duplicate records and keep tracking the latest challenge:

- Hash the title of the historical data and the hash title of the webscraped data
- Identity the new challenge by comparing those 2 hash
- Keep only records that are newer than the latest loaded challenge

### 3. Load

From the incremental data, processed records are loaded into a data warehouse (MotherDuck). Then, it will turn into a historical data.

- Database: wow_data
- Schema: main
- Table: wow_historic_data

### 4. Dashboard Publishing

After the whole data is existed in the data warehouse, need to store that data in Google Drive to update the Tableau dashboard on Tableau Public automatically.
Check this link for more info: https://help.tableau.com/current/pro/desktop/en-us/public_faq.htm#What9

- Create a connector in Python to connect to Google Drive
- Input the data into Tableau Desktop from Google Drive Connector
- Build a Tableau Dashboard with actions to interact the dashboard to get insights, filter the challenge

Link to the Tableau Public Dashboard: https://public.tableau.com/app/profile/le.luu/viz/Wow_tracking_challenges/WOWTableauTracker_copy

### 5. CI/CD Automation

After completing the pipeline, to keep the data is updated automatically, need an orchestration process. This project used Github Actions to automate the workflow. Need to set the yaml file to trigger the process.
Check this link: https://thedataschool.co.uk/le-luu/des-orchestration-and-trigger-setup/?ref=thedataschool.co.uk

In this project, Github actions will help to:

- Run the pipeline on a scheduled basis (every Thursday)
- Webscrape the WOW challenges every week
- Transform the data to be the same as the schema of the historical data on warehouse
- Only add the latest challenge and avoid duplicate challenge
- Load the latest data on warehouse and Google Drive
- Update the dashboard data source

### Tools

| Stage           | Tool              | Purpose                                                         |
| --------------- | ----------------- | --------------------------------------------------------------- |
| Extract         | Python            | Core scripting language                                         |
| Extract         | Selenium          | Scrape dynamic Workout Wednesday webpages                       |
| Extract         | Tableau Hyper API | Extract historical challenge data from Tableau Public workbooks |
| Transform       | Pandas            | Data cleaning and transformation                                |
| Transform       | Regex             | Extract week numbers and standardize dates                      |
| Data Warehouse  | MotherDuck        | Store historical and incremental challenge data                 |
| Database Engine | DuckDB            | Query and process analytical data                               |
| Load            | Google Drive API  | Store curated dataset for Tableau Public                        |
| Visualization   | Tableau Desktop   | Build and publish dashboard                                     |
| Automation      | GitHub Actions    | Schedule and automate ETL workflow                              |
| Version Control | Git & GitHub      | Source code management                                          |
