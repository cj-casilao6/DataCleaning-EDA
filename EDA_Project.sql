# EXPLORATORY DATA ANALYSIS (EDA): Use clean data to find insights

# ===========================================
# For now, just find certain trends/information to branch upon
# =========================================== 

-- Showcase potential new column to calculate total number (round down to nearest whole #) of employees at time of layoffs
SELECT *, FLOOR(total_laid_off / percentage_laid_off) AS total_employees
FROM mult_layoffs_staging
WHERE total_laid_off IS NOT NULL AND percentage_laid_off IS NOT NULL
ORDER by total_employees;

-- Find all companies that went under
SELECT *
FROM mult_layoffs_staging
WHERE percentage_laid_off = 1;

-- Find total number of employees laid off per company
SELECT company, SUM(total_laid_off) AS total_laid_off
FROM mult_layoffs_staging
WHERE total_laid_off IS NOT NULL
GROUP BY company
ORDER BY 2 DESC;

-- Find range of dates
SELECT MIN(date), MAX(DATE)		-- Pretty much start of COVID through until Mar 6, 2023
FROM mult_layoffs_staging;

-- Find total number of employees laid off per industry
SELECT industry, SUM(total_laid_off) AS total_laid_off
FROM mult_layoffs_staging
GROUP BY industry
ORDER BY 2 DESC;

-- Find total number of employees laid off per country
SELECT country, SUM(total_laid_off) AS total_laid_off
FROM mult_layoffs_staging
GROUP BY country
ORDER BY 2 DESC;

-- Find total number of employees laid off per year
SELECT YEAR(`date`) AS yr, SUM(total_laid_off) AS total_laid_off
FROM mult_layoffs_staging
GROUP BY yr;

-- Find total number of employees laid off per stage
-- STAGE: Refers to funding or ownership status of company at time of layoffs
	-- Seed: Very early. Usually small team and limited funding
	-- Series A-J: Venture capital funding grounds. Make way through Series as company prepares to go public
    -- POST-IPO: Company has had Initial Public offering and is now traded on stock market
    -- Private equity: Company was bought by private equity firm
    -- Acquired: Company bought by another compnay 
    -- Subsidiary: Company is a daughter company onwned by larger parent company
SELECT stage, SUM(total_laid_off) AS total_laid_off
FROM mult_layoffs_staging
GROUP BY stage
ORDER BY 2 DESC;


# ===========================================
# Do rolling total layoffs starting from 2020-03 to 2023-03
# =========================================== 

-- Get the total number of employees laid off per each combination of Year and Month
SELECT SUBSTRING(`date`, 1, 7) AS Year_and_Month, SUM(total_laid_off)
FROM mult_layoffs_staging
WHERE `date` IS NOT NULL
GROUP BY Year_and_Month
ORDER BY Year_and_Month;

-- Put query into CTE to extract rolling total through window function
WITH rolling_total_cte AS
(
	SELECT SUBSTRING(`date`, 1, 7) AS Year_and_Month, SUM(total_laid_off) AS Laid_Off_Per_Month
	FROM mult_layoffs_staging
	WHERE `date` IS NOT NULL
	GROUP BY Year_and_Month
	ORDER BY Year_and_Month
)
SELECT Year_and_Month, Laid_Off_Per_Month, SUM(Laid_Off_Per_Month) OVER(ORDER BY Year_and_Month) AS Rolling_Total
FROM rolling_total_cte;


-- Find total number of employees laid off per company
SELECT company, SUM(total_laid_off) AS total_laid_off
FROM mult_layoffs_staging
WHERE total_laid_off IS NOT NULL
GROUP BY company
ORDER BY 2 DESC;


# ===========================================
# Get the 5 companies with most layoffs per year
# =========================================== 

-- First CTE is meant to pair company with year and their total number of employees laid off
WITH company_cte (`Year`, `Company`, `Employees Laid Off`) AS
(
	SELECT YEAR(`date`) AS yr, company, SUM(total_laid_off) AS employees_off
	FROM mult_layoffs_staging
    WHERE `date` IS NOT NULL
    GROUP BY yr, company
) , company_year_rank_cte AS
-- Second CTE ranks each company based on their total number of employees laid off
(
	SELECT *, DENSE_RANK() OVER(PARTITION BY `Year` ORDER BY `Employees Laid Off` DESC) AS Ranking
	FROM company_cte
)
-- Finally, select everything but limit each instance of ranking to limit number of companies 
SELECT *
FROM company_year_rank_cte
WHERE Ranking <= 5;

