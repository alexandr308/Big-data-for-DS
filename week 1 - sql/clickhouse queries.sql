SELECT platform     -- select countIf(platform='android') as android
    , count(event)  --    , countIf(platform='ios') as ios
FROM ads_data
WHERE event = 'view'
GROUP BY platform;


SELECT count(DISTINCT platform) AS uniq_platforms --uniqExact(platform)
FROM ads_data
GROUP BY ad_id
ORDER BY uniq_platforms DESC LIMIT 1;


WITH (SELECT count(DISTINCT client_union_id) FROM ads_clients_data) AS clients
SELECT clients
    , uniqExact(client_union_id) AS clints_on_date --count(disctinct )
    , round(100 - clints_on_date / clients) percent
    , min(create_date) AS first_clients_date
FROM ads_clients_data
WHERE client_union_id GLOBAL NOT IN (
    SELECT client_union_id
    FROM ads_data
    )
;


SELECT min(md.date - clients.create_date) AS min_days
    , max(md.date - clients.create_date) AS max_days
    , round(avg(md.date - clients.create_date)) AS avg_days
FROM (
     SELECT min(date) AS date
          , client_union_id
     FROM ads_data AS active
     GROUP BY client_union_id
) AS md
JOIN ads_clients_data AS clients
    ON md.client_union_id = clients.client_union_id
;
