SELECT sum(salary)
FROM employee
WHERE (name = 'Morty' AND surname = 'Smith') OR (name = 'Rick' AND surname = 'Sanchez C-137');


SELECT round(avg(salary), 0)
FROM employee
WHERE job_title = 'Data Science';


SELECT count(1) AS number
      , name
FROM employee
WHERE company_id = 2
GROUP BY name
ORDER BY number DESC LIMIT 1;


SELECT count(1)
FROM employee
WHERE end_date IS NOT NULL
    AND company_id = 4; --1, 2, 3, 4


SELECT count(1) AS job_freq
    , job_title
FROM employee
GROUP BY job_title
ORDER BY job_freq DESC LIMIT 5;


SELECT country
    , count(job_title) AS number
FROM employee AS e
JOIN office AS o
    ON e.office_id = o.id
WHERE job_title LIKE '%DevOps%'
GROUP BY country
ORDER BY number DESC LIMIT 1;


SELECT id
     , job_title
     , date_part('day', end_date::timestamp - start_date::timestamp) AS work_days
FROM employee
WHERE id = (
    SELECT id
    FROM employee
    WHERE date_part('day', end_date::timestamp - start_date::timestamp) = (
        SELECT min(date_part('day', end_date::timestamp - start_date::timestamp))
        FROM employee
               )
    )
;


SELECT end_date
    , count(1) AS num_fires
FROM employee
WHERE end_date IS NOT NULL
GROUP BY end_date
ORDER BY num_fires DESC LIMIT 1;
