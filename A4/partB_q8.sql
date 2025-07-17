-- In the gender_jobs_data table - How much money (TOTAL_EARNINGS_FEMALE) did female workers make as engineers in 2016?
SELECT SUM(total_earnings_female) AS total_earnings_female
FROM gender_jobs_distribution
WHERE year = 2016 AND occupation like '%engineer%' ;
