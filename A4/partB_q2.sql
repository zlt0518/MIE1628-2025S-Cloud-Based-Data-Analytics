--In the gender_jobs_data table - How many OCCUPATIONS exist in the MINOR_CATEGORY of Business and Financial Operations overall?
SELECT COUNT(DISTINCT occupation)
FROM gender_jobs_distribution
WHERE minor_category = 'Business and Financial Operations';