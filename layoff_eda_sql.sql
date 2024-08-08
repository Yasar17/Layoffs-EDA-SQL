---DATA CLEANING 
   
	---1. Removing Duplicates
 	---2. Standardize the Data
	---3. Null valuse os blank values
	---4. Remove unused columns

CREATE TABLE layoffs_staging (
    LIKE layoffs
);

INSERT INTO layoffs_staging
	SELECT * 
	FROM layoffs;

SELECT * 
FROM layoffs_staging;

---1. Removing Duplicates

----Identifing Duplicates

WITH duplicate_cte AS
(
SELECT * ,
ROW_NUMBER() OVER(
	PARTITION BY company,location,industry , total_laid_off, percentage_laid_off ,date,stage,country, funds_raised_millions
	) AS row_num
	FROM layoffs_staging
)
SELECT *
from duplicate_cte 
where row_num > 1;

----Deleting Duplicates

WITH duplicate_cte AS (
    SELECT 
        ctid,  -- ctid is used to uniquely identify rows in the absence of a primary key
        ROW_NUMBER() OVER (
            PARTITION BY company, location, industry, total_laid_off, 
                         percentage_laid_off, date, stage, country, funds_raised_millions
            ORDER BY ctid
        ) AS row_num
    FROM 
        layoffs_staging
)
DELETE FROM layoffs_staging
WHERE ctid IN (
    SELECT ctid
    FROM duplicate_cte
    WHERE row_num > 1
);

---2. Standardize the Data
	
---Triming off space in company column
SELECT company,TRIM(company) 
FROM layoffs_staging

UPDATE layoffs_staging
SET company = TRIM(company);

---Updating "Crypto% " to  "Crypto"

SELECT distinct (industry)
FROM layoffs_staging
order by 1;

SELECT *
FROM layoffs_staging 
WHERE industry LIKE 'Crypto%' ; 

UPDATE layoffs_staging 
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%' ;

---Updating country column

SELECT distinct (country)
FROM layoffs_staging
where country LIKE 'United States%'
order by 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) AS trimmed_country
FROM layoffs_staging
ORDER BY country;

UPDATE layoffs_staging 
SET country = TRIM(TRAILING '.' FROM country )
WHERE country LIKE 'United States.';


---Changing column data type from text to date
	
SELECT date 
FROM layoffs_staging;

UPDATE layoffs_staging 
SET date = TO_DATE(date, 'YYYY-MM-DD')
WHERE date ~ '^\d{4}-\d{2}-\d{2}$';

ALTER TABLE layoffs_staging
ALTER COLUMN date TYPE DATE
USING TO_DATE(date, 'YYYY-MM-DD');

SELECT * 
FROM layoffs_staging;

---3. Handeling  Null values

SELECT *
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging
WHERE industry IS NULL 
OR industry = ''

SELECT * 
FROM layoffs_staging
WHERE company = 'Airbnb' ;
 
SELECT t1.industry ,t2.industry 
FROM layoffs_staging t1
JOIN layoffs_staging t2
ON t1.company = t2.company
AND t1.location = t2.location
WHERE t1.industry IS NULL OR t1.industry = ' '
AND t2.industry IS NOT NULL
order by 1

UPDATE layoffs_staging t1
SET industry = t2.industry
FROM layoffs_staging t2
WHERE t1.company = t2.company
  AND (t1.industry IS NULL OR t1.industry = ' ')
  AND t2.industry IS NOT NULL;

SELECT * 
FROM layoffs_staging
WHERE company LIKE 'Bally%' ;


SELECT *
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


---Final Cleaned table below.
SELECT  * FROM layoffs_staging


--EDA  Starts From Here
select *
from layoffs_staging;

-- list of COUNTRIES along with the total number of layoffs in each country,
---sorted in descending order by the total number of layoffs

SELECT country, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
WHERE country IS NOT NULL
GROUP BY country
ORDER BY total_laid_off DESC;

--- list of LOCATIONS along with the total number of layoffs in each location, 
-----sorted in descending order by the total number of layoffs.
SELECT location, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
WHERE location IS NOT NULL
GROUP BY location
ORDER BY total_laid_off DESC;

---Company-wise Layoffs
SELECT company, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
WHERE company IS NOT NULL
GROUP BY company
HAVING SUM(total_laid_off) IS NOT NULL
ORDER BY total_laid_off DESC;

---Industry-wise Layoffs
SELECT industry, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
WHERE industry IS NOT NULL
GROUP BY industry
HAVING SUM(total_laid_off) IS NOT NULL
ORDER BY total_laid_off DESC;

---list the years along with the total layoffs for each year, sorted in descending order of total layoffs.
SELECT EXTRACT(YEAR FROM date) AS year, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
GROUP BY year
HAVING SUM(total_laid_off) IS NOT NULL
ORDER BY total_laid_off DESC;


---Provide a time series analysis of layoffs on a monthly basis, and then compute a running total of layoffs.
SELECT TO_CHAR(date, 'YYYY-MM') AS month, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
WHERE date IS NOT NULL
GROUP BY month
ORDER BY month ASC;

with rolling_cte as (
	SELECT TO_CHAR(date, 'YYYY-MM') AS month, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
WHERE date IS NOT NULL
GROUP BY month
ORDER BY month ASC
)
SELECT month,total_laid_off,sum(total_laid_off) over(order by month)
from rolling_cte;


---provide SQL query performs a multi-step analysis to identify the top 5 companies with the highest layoffs for each year.
SELECT company,EXTRACT(YEAR FROM date) as year, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
WHERE company IS NOT NULL
GROUP BY company,year
HAVING SUM(total_laid_off) IS NOT NULL
ORDER BY 3 DESC;

with company_year as(
	SELECT company,EXTRACT(YEAR FROM date) as year, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
WHERE company IS NOT NULL
GROUP BY company,year
HAVING SUM(total_laid_off) IS NOT NULL
ORDER BY 3 DESC
), company_year_ranking as (
	select *,dense_rank() over(partition by year order by total_laid_off desc) as Ranking 
	from company_year
	where year is not null 
)
	SELECT * from company_year_ranking 
	where Ranking <=5; 




