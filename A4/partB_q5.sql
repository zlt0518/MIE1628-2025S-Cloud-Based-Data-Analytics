-- In the gender_jobs_data table - What were the total earnings of male (TOTAL_EARNINGS_MALE) employees in the Service MAJOR_CATEGORY for the year 2015?
SELECT SUM(total_earnings_male)
FROM gender_jobs_distribution
WHERE major_category = 'Service' AND year = 2015;