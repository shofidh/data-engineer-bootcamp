--# 1. The Report

SELECT
    CASE WHEN g.grade > 7 THEN s.name ELSE NULL END AS name,
    g.grade,
    s.marks
FROM students s
LEFT JOIN grades g ON s.marks BETWEEN g.min_mark AND g.max_mark
ORDER BY g.grade DESC, s.name, g.grade

--# 2. Weather Observation Station 18
WITH ds AS (
SELECT
    MIN(LAT_N) a,
    MIN(LONG_W) b,
    MAX(LAT_N) c,
    MAX(LONG_W) d
FROM station)
SELECT
    ROUND(ABS(a-c)+ABS(b-d),4)
FROM ds

--#3. Top Competitors
SELECT
    ha.hacker_id,
    ha.name
FROM submissions sub
INNER JOIN
    hackers ha ON ha.hacker_id = sub.hacker_id
INNER JOIN
    challenges ch ON ch.challenge_id = sub.challenge_id
INNER JOIN
    difficulty dif ON dif.difficulty_level = ch.difficulty_level
WHERE 1=1
    AND sub.score = dif.score
    AND ch.difficulty_level = dif.difficulty_level
GROUP BY hacker_id, name
HAVING COUNT(sub.hacker_id) > 1
ORDER BY COUNT(sub.hacker_id) DESC, hacker_id

--#4. Ollivander's Inventory
WITH wand_data AS (
    SELECT
        w.id,
        wp.age,
        w.power,
        w.coins_needed,
        MIN(w.coins_needed) OVER (PARTITION BY wp.age, w.power) AS coins_min
    FROM Wands w
    JOIN Wands_Property wp
        ON w.code = wp.code
    WHERE wp.is_evil = 0
)
SELECT
    id,
    age,
    coins_needed,
    power
FROM wand_data
WHERE coins_needed = coins_min
ORDER BY power DESC, age DESC

--#5. Contest Leaderboard
WITH max_score AS (
    SELECT
        ha.hacker_id,
        sub.challenge_id,
        ha.name,
        MAX(sub.score) AS score
    FROM submissions sub
    JOIN hackers ha ON ha.hacker_id = sub.hacker_id
    GROUP BY ha.hacker_id, sub.challenge_id, ha.name
)
SELECT
    hacker_id,
    name,
    SUM(score) AS total_score
FROM max_score
GROUP BY hacker_id, name
HAVING SUM(score) > 0
ORDER BY SUM(score) DESC, hacker_id;
