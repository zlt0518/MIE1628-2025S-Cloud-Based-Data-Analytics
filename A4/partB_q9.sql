-- What is the total number of full-time and part-time female workers versus male workers year over year?
SELECT year, SUM(full_time_male) AS total_full_time_male_workers, SUM(part_time_male) AS total_part_time_male_workers, SUM(full_time_female) AS total_full_time_female_workers, SUM(part_time_female) AS total_part_time_female_workers
FROM gender_jobs_distribution
GROUP BY year;