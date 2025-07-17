-- In the gender_jobs_data table - How many female workers were in management roles in the year 2015?
SELECT SUM(workers_female)
FROM gender_jobs_distribution
WHERE minor_category = 'Management' AND year = 2015;
