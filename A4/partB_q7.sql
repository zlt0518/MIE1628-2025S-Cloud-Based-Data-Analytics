-- In the gender_jobs_data table - Compare the TOTAL_EARNINGS_MALE and TOTAL_EARNINGS_FEMALE earnings irrespective of occupation by each year
SELECT year,SUM(total_earnings_male) AS total_earnings_male, SUM(total_earnings_female) AS total_earnings_female
FROM gender_jobs_distribution
GROUP BY year;
