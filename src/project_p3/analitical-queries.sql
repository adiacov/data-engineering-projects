-- These are queries to run against 'project-3'.

-- ANALYTICAL QUERY: Collisions per month
SELECT
    year,
    month,
    COUNT(*) AS collisions_per_month
FROM
    collisions_fact f
    JOIN collisions_dim_date d ON f.date_key = d.date_key
GROUP BY
    year,
    month
ORDER BY year, month;

-- ANALYTICAL QUERY: Severity distribution in two ways:
-- 1. using only the fact table
-- 2. using both, fact and dimension table
-- What's the difference between the two approaches?
-- 1. You can have an aggregated result, but no details
-- 2. You can have the same aggregated result, with more details,
--    e.g. severity group or description (more meaningful result)

-- Severity distribution (using only collisions_fact)
SELECT
    severity_key,
    COUNT(*) AS severity_count_per_group
FROM collisions_fact
GROUP BY
    severity_key;

-- Severity distribution (using collisions_fact and collisions_dim_severity)
SELECT
    severity_group,
    COUNT(*) AS severity_count_per_group
FROM
    collisions_fact f
    JOIN collisions_dim_severity s ON f.severity_key = s.severity_key
GROUP BY
    s.severity_group;

-- ANALYTICAL QUERY: Weekend vs weekday collisions
-- Here I use the 'collision_count' column rather than COUNT(*),
-- this is closer to the reality when the aggregation is done
-- on numerical values (remember, collision_count is 1 always for simplicity)

-- Weekday collisions only
SELECT COUNT(collision_count) AS weekday_collisions_count
FROM
    collisions_fact f
    JOIN collisions_dim_date d ON f.date_key = d.date_key
WHERE
    is_weekend = False;

-- Weekend collisions only
SELECT COUNT(collision_count) AS weekend_collisions_count
FROM
    collisions_fact f
    JOIN collisions_dim_date d ON f.date_key = d.date_key
WHERE
    d.is_weekend = True;

-- All together: weekday, weekend collisions
SELECT
    SUM(
        CASE
            WHEN d.is_weekend = False THEN 1
        END
    ) AS weekday_collisions_count,
    SUM(
        CASE
            WHEN d.is_weekend = True THEN 1
        END
    ) AS weekend_collisions_count,
    COUNT(collision_count) AS total_collisions
FROM
    collisions_fact f
    JOIN collisions_dim_date d ON f.date_key = d.date_key;

-- ANALYTICAL QUERY: Peak collision hours

SELECT t.hour, COUNT(collision_count) AS collisions
FROM
    collisions_fact f
    JOIN collisions_dim_time t ON f.time_key = t.time_key
GROUP BY
    t.hour
HAVING
    collisions > 7000
ORDER BY collisions DESC;