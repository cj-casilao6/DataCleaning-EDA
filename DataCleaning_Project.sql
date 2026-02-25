# DATA CLEANING: Transform raw data into usable/standardized data

# STEPS FOR DATA CLEANING:
	-- 0) Create a duplicate table of dataset to avoid manipulating the imported table
    -- 1) Identify & delete duplicate row(s)
    -- 2) Standardizing data
    -- 3) Blank/NULL values
    -- 4) Remove unnecessary column(s)
    
SELECT *
FROM layoffs;
    
    
# ===========================================
# STEP 0: Create a duplicate table
# ===========================================

-- Create duplicate staging table with all the same column names from imported table
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Confirm staging table exists with correct column names
SELECT *
FROM layoffs_staging;

-- Populate the staging table with the original data from the imported table
INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

-- Confirm staging table has been populated with the correct values
SELECT *
FROM layoffs_staging;


# ===========================================
# STEP 1: Identify & delete duplicate row(s)
# ===========================================

-- Create CTE
WITH layoffs_cte AS
(
	-- Chosen columns that would stay the same to indicate repeated row
    -- New column made to indicate if this combination repeats. If any row value > 1 then repeated information
	SELECT *, ROW_NUMBER() OVER(PARTITION BY
		company, industry, total_laid_off, percentage_laid_off, `date`) AS num_layoffs
    FROM layoffs_staging
)
SELECT *
FROM layoffs_cte
WHERE num_layoffs > 1;	-- Only return those rows where window function column returns value > 1 (duplicate row)

-- Due to inability to directly modify CTE table, create duplicate staging table with this information to delete properly
	-- Copied to clipboard from left window (layoffs_staging -> Copy  to Clipboard -> Create Statement)
CREATE TABLE `mult_layoffs_staging` (	-- Give new name to table
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `num_layoffs` int DEFAULT NULL				-- Add new column to account for window function ROW_NUMBER()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Confirm new table exists with all column names
SELECT *
FROM mult_layoffs_staging;

-- Insert original data AND new data from the window function ROW_NUMBER() into the new table
INSERT INTO mult_layoffs_staging
SELECT *, ROW_NUMBER() OVER(PARTITION BY
		company, industry, total_laid_off, percentage_laid_off, `date`) AS num_layoffs
FROM layoffs_staging;

-- Confirm new table has expected data
SELECT *
FROM mult_layoffs_staging;

-- Once again, retrieve the rows for companies with duplicate data rows
SELECT *
FROM mult_layoffs_staging
WHERE num_layoffs > 1;

-- Delete those rows from the table
DELETE 
FROM mult_layoffs_staging
WHERE num_layoffs > 1;			

-- (SANITY CHECK) Ensure that repeated rows were actually deleted
SELECT *
FROM mult_layoffs_staging
WHERE num_layoffs > 1;

# SIDE NOTE: Will have to go back and delete num_layoffs column as it's not needed anymore (STEP 4)


# ===========================================
# STEP 2: Standardizing Data
# =========================================== 

# -------------------------------------------
# STEP 2a: Standardize 'company' column
# -------------------------------------------

-- Identify companies with unnecessary white space
SELECT company
FROM mult_layoffs_staging
WHERE company LIKE ' %' OR company LIKE '% ';

-- Replace old company name with trimmed name (remove leading and/or trailing whitespaces)
UPDATE mult_layoffs_staging
SET company = TRIM(company);		-- After execution, rerun above query for SANITY CHECK (should return nothing)

# -------------------------------------------
# STEP 2b: Standardize 'industry' column
# -------------------------------------------

-- Observe every indsutry name; look for any semi-related names that could be standardized
	-- Reuslt shows blank value, NULL, and similar Crypto industries (Cyrpto, CryptoCurrency, & Crypto Currency)
SELECT DISTINCT(industry)
FROM mult_layoffs_staging
ORDER BY industry;

SELECT DISTINCT(industry)
FROM mult_layoffs_staging
WHERE industry LIKE 'Crypto%';

-- Only rename Crypto-esque values to one standard value (will handle NULL and BLANK value later)
UPDATE mult_layoffs_staging
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';		-- After execution, rerun above query for SANITY CHECK (should return only 'Crypto')

# -------------------------------------------
# STEP 2c: Standardize 'location' column
# -------------------------------------------

-- Looks alright for the most part (some weird characters but may be a different language?)
SELECT DISTINCT(location)
FROM mult_layoffs_staging
ORDER BY location;

# -------------------------------------------
# STEP 2d: Standardize 'total_laid_off' column
# -------------------------------------------

-- Result shows a NULL value. The rest looks OKAY
SELECT DISTINCT(total_laid_off)
FROM mult_layoffs_staging
ORDER BY 1;

# -------------------------------------------
# STEP 2e: Standardize 'country' column
# -------------------------------------------

-- Result includes 'United States' & 'United States.'
SELECT DISTINCT(country)
FROM mult_layoffs_staging
ORDER BY 1;

-- Update row value to one standardized 'United States'
	-- Can also do: TRIM(TRALINING '.' FROM country)
UPDATE mult_layoffs_staging
SET country = 'United States'
WHERE country LIKE 'United States%';	-- After execution, rerun above query for SANITY CHECK

# -------------------------------------------
# STEP 2f: Standardize 'date' column
# -------------------------------------------
-- When importing the excel file, date was automatically assigned 'text' variable type. Change to 'date' variable type

-- Read original 'date' col and format it to proper date notation
SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y')		-- %m = Month number, %d = Day number, %Y = Full year
FROM mult_layoffs_staging;

-- Update 'date' column to be of proper date notation
UPDATE mult_layoffs_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- (SANITY CHECK) View date column and ensure values look okay
SELECT `date`
FROM mult_layoffs_staging
ORDER BY 1;

-- Now able to change variable type of entire column from text -> date
ALTER TABLE mult_layoffs_staging
MODIFY COLUMN `date` DATE;


# ===========================================
# STEP 3: Handle NULL / blank values
# =========================================== 

-- Reference blank/NULL values found prior within 'industry' column
SELECT *
FROM mult_layoffs_staging
WHERE industry = '' OR industry IS NULL;	-- Airbnb, Bally's Interactive, Carvana, Juul

-- Change blank values to be NULL for easier removal logic later
UPDATE mult_layoffs_staging
SET industry = NULL
WHERE industry = '';

-- Self join to find other instances of same company with industry to populate value with proper industry
SELECT st1.industry, st2.industry
FROM mult_layoffs_staging st1
JOIN mult_layoffs_staging st2
	ON st1.company = st2.company
WHERE st1.industry IS NULL AND st2.industry IS NOT NULL;

-- Update the blank/NULL 'industry' values with pre-existing 'industry' values from the same company (from different row)
UPDATE mult_layoffs_staging st1
JOIN mult_layoffs_staging st2
	ON st1.company = st2.company
SET st1.industry = st2.industry
WHERE (st1.industry = '' OR st1.industry IS NULL) AND (st2.industry IS NOT NULL);

-- (SANITY CHECK) Recheck updated industry column for NULL or blank values
SELECT *
FROM mult_layoffs_staging
WHERE industry = '' OR industry IS NULL;	-- Bally's Interactive

-- Look for any other row(s) of Bally's Interactive to populate industry
SELECT *
FROM mult_layoffs_staging
WHERE company = "Bally's Interactive";		-- No other row, leave as is

# SIDE NOTE: Cannot populate other columns with NULL values with only the information given to us


# ===========================================
# STEP 4: Remove unnecessary column(s) / row(s)
# =========================================== 

-- Find potential rows to remove where information about total/percentage laid off is NULL (row not needed)
SELECT *
FROM mult_layoffs_staging
WHERE total_laid_off IS NULL		-- CANNOT do 'WHERE total_laid_off = NULL' b/c NULL isn't a specific value
AND percentage_laid_off IS NULL;

-- Due to insufficient data, delete the rows
DELETE
FROM mult_layoffs_staging
WHERE total_laid_off IS NULL		
AND percentage_laid_off IS NULL;

SELECT *
FROM mult_layoffs_staging;

-- Remove the now unnecessary column from the window function in previous step
ALTER TABLE mult_layoffs_staging
DROP COLUMN num_layoffs;

-- Upon further inspection, there is a report of 0 percentage_laid_off when total_laid_off is not 0
SELECT *
FROM mult_layoffs_staging
WHERE total_laid_off IS NOT NULL AND percentage_laid_off IS NOT NULL
ORDER by percentage_laid_off;

SELECT *
FROM mult_layoffs_staging
WHERE percentage_laid_off = 0;

-- Remove this row as it doesn't make sense
DELETE FROM mult_layoffs_staging
WHERE percentage_laid_off = 0;


# =====================================================================================================================
# DATA IS NOW CLEAN. Move onto Exploratory Data Analysis (EDA)
# =====================================================================================================================
