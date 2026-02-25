# DataCleaning-EDA
Start by getting a raw Excel file and then importing it into MySQL. Started by cleaning data using by creating multiple staging tables paired with SQL queries all meant to clean the data. After cleaning the data, move onto EDA by creating more SQL queries that find important information regarding numerous companies that underwent layoffs.

# Raw Excel Data
Firstly, retrieve a dirty excel table found on the internet. Then import that excel file into MySQL using it's 'Table Data Import Wizard'. 

* Total number of rows: 2362 -- (Below is a sample of the raw data)

<img width="673" height="783" alt="image" src="https://github.com/user-attachments/assets/61857c5d-619f-4c97-ad8b-387a399040f8" />

# Data Cleaning
After importing the raw excel table into MySQL, perform multiple SQL queries with the ultimate goal to standardize/clean data. 

**Steps for Data Cleaning:**

0) Create a duplicate table of dataset to avoid manipulating the imported table
1) Identify & delete duplicate row(s)
2) Standardizing data
3) Handle blank/NULL values
4) Remove unnecessary column(s)

(Below is a sample of the cleaned data within MySQL)
<img width="887" height="727" alt="image" src="https://github.com/user-attachments/assets/267bbd0c-51b0-4d2f-a219-fc9f4f0ca659" />

# Exploratory Data Analysis 
Now that the data is clean, find valuable insights that would reveal some information about the companies that underwent layoffs.

(Below are some examples of trends found)

**Top 5 companies with most layoffs per year**:

<img width="347" height="367" alt="image" src="https://github.com/user-attachments/assets/fe0f697f-d91d-4c1b-b309-3da1d92ef547" />

**Rolling total layoffs from start of data to end**:

<img width="330" height="580" alt="image" src="https://github.com/user-attachments/assets/7f2c228a-ea78-4bc9-8a38-4678f03f87f9" />

**Total number of employees laid off per company stage**:

<img width="184" height="293" alt="image" src="https://github.com/user-attachments/assets/acf3ad0d-385e-4b4c-9094-0a21edb7ca7e" />
