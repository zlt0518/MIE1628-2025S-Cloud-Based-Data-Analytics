-- In the gender_jobs_data table - Summarize the total number of WORKERS_FEMALE in the MAJOR_CATEGORY of Management, Business, and Financial by each year.
SELECT year, SUM(workers_female)
FROM gender_jobs_distribution
WHERE major_category = 'Management, Business, and Financial '
GROUP BY year;
