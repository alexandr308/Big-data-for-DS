SELECT date
    , count(event) AS num_events
    , countIf(event = 'view') AS num_views
    , countIf(event = 'click') AS num_clicks
    , count(DISTINCT ad_id) AS uniq_ads--uniqExact(ad_id)
    , count(DISTINCT campaign_union_id) AS uniq_campaigns
FROM ads_data
GROUP BY date
ORDER BY date ASC;


SELECT date
    , ad_id
    , count(event) AS num_events
    , countIf(event = 'view') AS num_views
    , countIf(event = 'click') AS num_clicks
FROM ads_data
GROUP BY date, ad_id
ORDER BY num_events DESC, num_views DESC, num_clicks DESC LIMIT 5;


SELECT date
    , max(ad_id) AS ad
    , count(event) AS num_events
    , countIf(event = 'view') AS num_views
    , countIf(event = 'click') AS num_clicks
FROM ads_data
WHERE ad_id = 112583
GROUP BY date;


SELECT ad_id
    , round(countIf(event = 'click') / countIf(event = 'view'), 4) AS CTR
    , countIf(event = 'click') AS clicks
    , countIf(event = 'view') AS views
FROM ads_data
GROUP BY ad_id
HAVING views != 0
ORDER BY CTR DESC LIMIT 10
;


SELECT round(median(CTR), 4) AS median_CTR
    , round(avg(CTR), 4) AS avg_CTR
FROM (
    SELECT round(countIf(event = 'click') / countIf(event = 'view'), 4) AS CTR
        , countIf(event = 'view') AS views
    FROM ads_data
    GROUP BY ad_id
    HAVING views != 0
)
;


SELECT count(1) AS num_ads
FROM (
    SELECT ad_id
        , countIf(event = 'view') AS views
    FROM ads_data
    GROUP BY ad_id
    HAVING views == 0
)
;


SELECT ad_id
    , date
    , countIf(event = 'click') AS clicks
    , countIf(event = 'view') AS views
    , uniqExact(platform) AS num_platfroms
    , countIf(platform = 'android') AS android
    , countIf(platform = 'ios') AS ios
    , countIf(platform = 'web') AS web
    , sum(has_video) AS has_video
FROM ads_data
GROUP BY ad_id, date
HAVING views == 0;


SELECT round(median(CTR), 4) AS median_CTR
    , round(avg(CTR), 4) AS avg_CTR
FROM (
    SELECT round(countIf(event = 'click') / countIf(event = 'view'), 4) AS CTR
        , countIf(event = 'view') AS views
    FROM ads_data
    GROUP BY ad_id, has_video
    HAVING views != 0 AND has_video = 0
)
;


SELECT round(median(CTR), 4) AS median_CTR
    , round(avg(CTR), 4) AS avg_CTR
FROM (
    SELECT round(countIf(event = 'click') / countIf(event = 'view'), 4) AS CTR
        , countIf(event = 'view') AS views
    FROM ads_data
    GROUP BY ad_id, has_video
    HAVING views != 0 AND has_video = 1
)
;


SELECT round(quantile(0.95)(CTR), 4) AS quantile_95
FROM (
    SELECT round(countIf(event = 'click') / countIf(event = 'view'), 4) AS CTR
        , countIf(event = 'view') AS views
    FROM ads_data
    GROUP BY ad_id, date
    HAVING views != 0 AND date = '2019-04-04'
)
;


SELECT date
    , round(sumIf(ad_cost, ad_cost_type = 'CPC' AND event = 'click'), 2) AS cpc_cost
    , round(sumIf(ad_cost / 1000, ad_cost_type = 'CPM' AND event = 'view'), 2) AS cpm_cost
    , cpc_cost + cpm_cost AS margin
FROM ads_data
GROUP BY date
ORDER BY margin;


WITH (SELECT count(1) FROM ads_data WHERE event = 'view') AS total_ads
SELECT platform
    , count(platform) AS num_ads
    , round((num_ads / total_ads * 100) ,2) AS num_ads_percents
FROM ads_data
WHERE event = 'view'
GROUP BY platform
ORDER BY num_ads DESC;


SELECT ad_id
    , minIf(time, event = 'click') AS click_time
    , minIf(time, event = 'view') AS view_time
FROM ads_data
GROUP BY ad_id
HAVING click_time < view_time
    AND click_time != '1970-01-01 00:00:00';
