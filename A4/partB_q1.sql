-- In the gender_jobs_data table - Filter all the OCCUPATIONS in MAJOR_CATEGORY of Computer, Engineering, and Science for the YEAR 2013
SELECT *
FROM gender_jobs_distribution
WHERE MAJOR_CATEGORY = 'Computer, Engineering, and Science' AND YEAR = 2013;