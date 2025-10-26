
-- Data Quality (DQ) Queries for Netflix Dataset
-- Project ID: mgmt-467

-- 5.1 Missingness (users) - Query 1
-- Calculate total rows and percentage of missing values for country, subscription_plan, and age.
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS percent_missing_country,
    SUM(CASE WHEN subscription_plan IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS percent_missing_subscription_plan,
    SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS percent_missing_age
FROM
    `mgmt-467.netflix.users`;

-- 5.1 Missingness (users) - Verification Query (Missingness percentages rounded)
WITH base AS (
  SELECT COUNT(*) n,
         COUNTIF(country IS NULL) miss_country,
         COUNTIF(subscription_plan IS NULL) miss_plan,
         COUNTIF(age IS NULL) miss_age
  FROM `mgmt-467.netflix.users`
)
SELECT
       ROUND(100*miss_country/n,2) AS pct_missing_country,
       ROUND(100*miss_plan/n,2)   AS pct_missing_subscription_plan,
       ROUND(100*miss_age/n,2)    AS pct_missing_age
FROM base;

-- 5.2 Duplicates (watch_history) - Query 1 (Report duplicate groups)
SELECT user_id, movie_id, watch_date, device_type, COUNT(*) AS dup_count
FROM `mgmt-467.netflix.watch_history`
GROUP BY user_id, movie_id, watch_date, device_type
HAVING dup_count > 1
ORDER BY dup_count DESC
LIMIT 20;

-- 5.2 Duplicates (watch_history) - Query 2 (Create deduplicated table)
CREATE OR REPLACE TABLE `mgmt-467.netflix.watch_history_dedup` AS
SELECT * EXCEPT(rk) FROM (
  SELECT h.*,
         ROW_NUMBER() OVER (
           PARTITION BY user_id, movie_id, watch_date, device_type
           ORDER BY progress_percentage DESC, watch_duration_minutes DESC
         ) AS rk
  FROM `mgmt-467.netflix.watch_history` h
)
WHERE rk = 1;

-- 5.2 Duplicates (watch_history) - Verification Query (Before/after count)
SELECT
  (SELECT COUNT(*) FROM `mgmt-467.netflix.watch_history`) AS raw_count,
  (SELECT COUNT(*) FROM `mgmt-467.netflix.watch_history_dedup`) AS dedup_count;

-- 5.3 Outliers (minutes_watched) - Query 1 (Compute IQR bounds and % outliers)
WITH
  quantiles AS (
    SELECT
      APPROX_QUANTILES(watch_duration_minutes, 4)[OFFSET(1)] AS q1,
      APPROX_QUANTILES(watch_duration_minutes, 4)[OFFSET(3)] AS q3
    FROM
      `mgmt-467.netflix.watch_history_dedup`
  ),
  bounds AS (
    SELECT
      q1,
      q3,
      q1 - 1.5 * (q3 - q1) AS lower_bound,
      q3 + 1.5 * (q3 - q1) AS upper_bound
    FROM
      quantiles
  )
SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN watch_duration_minutes < b.lower_bound OR watch_duration_minutes > b.upper_bound THEN 1 ELSE 0 END) AS outlier_count,
  SUM(CASE WHEN watch_duration_minutes < b.lower_bound OR watch_duration_minutes > b.upper_bound THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS outlier_percentage
FROM
  `mgmt-467.netflix.watch_history_dedup`, bounds b;

-- 5.3 Outliers (minutes_watched) - Query 2 (Create robust table with capped values)
CREATE OR REPLACE TABLE `mgmt-467.netflix.watch_history_robust` AS
SELECT
  *, -- Select all original columns
  -- Cap watch_duration_minutes at P01 and P99 values (hardcoded from previous execution for example)
  CASE
    WHEN watch_duration_minutes < 4.4 THEN 4.4 -- Replace with actual P01_val
    WHEN watch_duration_minutes > 366.0 THEN 366.0 -- Replace with actual P99_val
    ELSE watch_duration_minutes
  END AS watch_duration_minutes_capped
FROM
  `mgmt-467.netflix.watch_history_dedup`;

-- 5.3 Outliers (minutes_watched) - Verification Query (Min/Median/Max before vs after capping)
SELECT
  'Before Capping' AS stage,
  MIN(watch_duration_minutes) AS min_minutes,
  APPROX_QUANTILES(watch_duration_minutes, 2)[OFFSET(1)] AS median_minutes,
  MAX(watch_duration_minutes) AS max_minutes
FROM
  `mgmt-467.netflix.watch_history_dedup`

UNION ALL

SELECT
  'After Capping' AS stage,
  MIN(watch_duration_minutes_capped) AS min_minutes,
  APPROX_QUANTILES(watch_duration_minutes_capped, 2)[OFFSET(1)] AS median_minutes,
  MAX(watch_duration_minutes_capped) AS max_minutes
FROM
  `mgmt-467.netflix.watch_history_robust`;

-- 5.4 Business anomaly flags - Query 1 (Summarize flag_binge)
SELECT
  COUNT(*) AS total_sessions,
  SUM(CASE WHEN watch_duration_minutes_capped > (8 * 60) THEN 1 ELSE 0 END) AS binge_sessions_count,
  SUM(CASE WHEN watch_duration_minutes_capped > (8 * 60) THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS binge_sessions_percentage
FROM
  `mgmt-467.netflix.watch_history_robust`;

-- 5.4 Business anomaly flags - Query 2 (Summarize flag_age_extreme)
SELECT
  COUNT(*) AS total_users,
  SUM(CASE WHEN age < 10 OR age > 100 THEN 1 ELSE 0 END) AS extreme_age_count,
  SUM(CASE WHEN age < 10 OR age > 100 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS extreme_age_percentage
FROM
  `mgmt-467.netflix.users`
WHERE age IS NOT NULL;

-- 5.4 Business anomaly flags - Query 3 (Summarize flag_duration_anomaly)
SELECT
  COUNT(*) AS total_movies,
  SUM(CASE WHEN duration_minutes < 15 OR duration_minutes > 480 THEN 1 ELSE 0 END) AS duration_anomaly_count,
  SUM(CASE WHEN duration_minutes < 15 OR duration_minutes > 480 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS duration_anomaly_percentage
FROM
  `mgmt-467.netflix.movies`
WHERE duration_minutes IS NOT NULL;

-- 5.4 Business anomaly flags - Verification Query (Compact summary)
WITH
  binge_flags AS (
    SELECT
      'flag_binge' AS flag_name,
      SUM(CASE WHEN watch_duration_minutes_capped > (8 * 60) THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_of_rows
    FROM
      `mgmt-467.netflix.watch_history_robust`
  ),
  age_flags AS (
    SELECT
      'flag_age_extreme' AS flag_name,
      SUM(CASE WHEN age < 10 OR age > 100 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_of_rows
    FROM
      `mgmt-467.netflix.users`
    WHERE age IS NOT NULL
  ),
  duration_flags AS (
    SELECT
      'flag_duration_anomaly' AS flag_name,
      SUM(CASE WHEN duration_minutes < 15 OR duration_minutes > 480 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_of_rows
    FROM
      `mgmt-467.netflix.movies`
    WHERE duration_minutes IS NOT NULL
  )
SELECT * FROM binge_flags
UNION ALL
SELECT * FROM age_flags
UNION ALL
SELECT * FROM duration_flags;
