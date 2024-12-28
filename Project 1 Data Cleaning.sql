-- Project 1: Data Cleaning

-- Create database
-- "add schema"

-- Import dataset
-- "table import wizard" under "sys"
-- double-click on schema "world_layoffs" to set as current schema


-- Clean data

SELECT *
FROM layoffs;

-- 1. Remove duplicates
-- 2. Standardize data
-- 3. Null/Blank values
-- 4. Remove and columns

-- Copy data to staging table
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging
SELECT * 
FROM layoffs;


-- Remove duplicates


-- Create CTE to filter duplicates
WITH duplicate_cte AS
(
SELECT *, 
# Create Row ID
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;
# Cannot delete from CTE because it is temporary



-- Create table and delete instances where row number = 2
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` INT  -- Create new column in staging table that virtually counts duplicates
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging2;

# Tip: use "SELECT *" to identify what you are deleting, then change to "DELETE"
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

# Note: ^^ threw error that safe update = on (restricts data from being deleted/updated)
SET SQL_SAFE_UPDATES = 0;
# or go to preferences > MySQL Editor > bottom of page



-- 2. Standardizing Data
-- Finding issues in data and fixing out

# remove whitespace from beginning and end of company name
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

# Standardize "Crypto-" industry label

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'; # find all instances of different Crypto- labels

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

# Standardize location

SELECT DISTINCT location
from layoffs_staging2
ORDER BY 1; # All good

# Standardize country
SELECT DISTINCT country
from layoffs_staging2
ORDER BY 1;

SELECT *
from layoffs_staging2
WHERE country LIKE 'United States';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States';

# Standardize time series of date column
# right now, date is type Text
# change to type Date

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y') # Function invocation: STR_TO_DATE(column, date type of existing column)
FROM layoffs_staging2;

SELECT `date`
FROM layoffs_staging2
WHERE `date` = 'NULL';

UPDATE layoffs_staging2
SET `date` = NULL
WHERE `date` = 'NULL';

SELECT `date`
FROM layoffs_staging2;
#WHERE `date` LIKE 'NULL';

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;




-- 3. Null and Blank Values
SELECT *
FROM layoffs_staging2
WHERE industry = 'NULL'
OR industry = '' OR industry = ' ' OR industry IS NULL;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Airbnb';

# standardize "NULLS" (json standard), blanks, and spaces to all be nulls
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = 'NULL' OR industry = '' OR industry = ' ';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL; # For CSVs, it would be "IS NULL" or "IS NOT NULL"

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Fix null without any duplicate industry info (go on internet to populate data)
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally';


# Delete rows where there is no info (no data for total_laid_off AND percentage_laid_off)
# These rows may not be useful to us
SELECT *
FROM layoffs_staging2
WHERE (total_laid_off = 'NULL' OR total_laid_off = '' OR total_laid_off IS NULL)
AND (percentage_laid_off = 'NULL' OR percentage_laid_off = '' OR percentage_laid_off IS NULL);
## Ask: should we delete this?

DELETE
FROM layoffs_staging2
WHERE (total_laid_off = 'NULL' OR total_laid_off = '' OR total_laid_off IS NULL)
AND (percentage_laid_off = 'NULL' OR percentage_laid_off = '' OR percentage_laid_off IS NULL);

SELECT *
FROM layoffs_staging2;

# Delete row_num column used to remove duplicates
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;


