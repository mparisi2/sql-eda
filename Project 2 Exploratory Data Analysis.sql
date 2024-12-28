-- Project 2: Exploratory Data Analysis

SELECT *
FROM layoffs_staging2;

## Change some data types that got messed up when converting to JSON file
UPDATE layoffs_staging2
SET total_laid_off = NULL
WHERE total_laid_off = 'NULL';

UPDATE layoffs_staging2
SET percentage_laid_off = NULL
WHERE percentage_laid_off = 'NULL';

UPDATE layoffs_staging2
SET funds_raised_millions = NULL
WHERE funds_raised_millions = 'NULL';

ALTER TABLE layoffs_staging2
MODIFY COLUMN total_laid_off INT;

ALTER TABLE layoffs_staging2
MODIFY COLUMN funds_raised_millions INT;



# Exploratory data analysis
# Who had the most people laid off on one day (by total workers and by percentage of workers)?
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

# Who went under, ordered by who had the most funding?
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1 # means entire company went under
ORDER BY funds_raised_millions DESC; 

# What company had the most layoffs?
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

# What is our time range of our data?
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

# What industry had the most layoffs?
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

# What country had the most layoffs?
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

# How many people laid off per year?
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

# How many people laid off in each stage of funding?
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
-- ORDER BY 1 DESC;
ORDER BY 2 DESC;



# Rolling total of layoffs by month
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, 
SUM(total_laid_off)
FROM layoffs_staging2
WHERE `date` IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

# Create CTE
WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, 
SUM(total_laid_off) AS total_off_per_month
FROM layoffs_staging2
WHERE `date` IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off_per_month,
SUM(total_off_per_month) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;


# Company layoffs per year
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

-- Who laid off the most per year?
# Create CTE of company layoffs per year
WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC
), 
# Create CTE ordering largest company layoffs per year
Company_Year_Rank AS
(SELECT *, 
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) as Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
# Take top 5 largest company layoffs per year
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;