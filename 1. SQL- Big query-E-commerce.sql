
-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL

SELECT
    FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) month
    ,SUM(totals.visits) visits
    ,SUM(totals.pageviews) pageviews,
    ,SUM(totals.transactions) transactions
    ,SUM(totals.totaltransactionRevenue)/1000000 revenue
FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
GROUP BY 1   
ORDER BY 1

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL

SELECT
  trafficSource.source
  ,SUM(totals.visits) totals_visits
  ,SUM(totals.bounces) total_no_of_bounces
  ,SUM(totals.bounces)*100/SUM(totals.visits) bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY trafficSource.source
ORDER BY totals_visits desc;
  

-- Query 3: Revenue by traffic source by week, by month in June 2017

WITH Month as (
      SELECT 'month' as time_type
            ,FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) time
            ,trafficSource.source
            ,SUM(totals.totalTransactionRevenue)/1000000 revenue
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
      GROUP BY 3,2
)
,

Week as(
      SELECT 'week' as time_type
            ,FORMAT_DATE("%Y%W",PARSE_DATE("%Y%m%d",date)) time
            ,trafficSource.source
            ,SUM(totals.totalTransactionRevenue)/1000000 revenue
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
      GROUP BY 3,2
)

SELECT *
FROM Month
UNION ALL
SELECT *
FROM Week
ORDER BY revenue DESC;


--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL


WITH purchase AS(
      SELECT 
            FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) month   
            ,SUM(totals.pageviews)/COUNT(DISTINCT fullVisitorId) avg_pageviews_purchase
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
      WHERE  totals.transactions>=1
      AND  _table_suffix between '0601' and '0731'
      GROUP BY 1
)
,

non_purchase AS(
      SELECT 
            FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) month  
            ,SUM(totals.pageviews)/COUNT(DISTINCT fullVisitorId) avg_pageviews_non_purchase
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
      WHERE  totals.transactions IS NULL
      AND _table_suffix between '0601' and '0731'
      GROUP BY 1
)

SELECT p.month
       ,avg_pageviews_purchase
       ,avg_pageviews_non_purchase
FROM purchase as p
INNER JOIN non_purchase as n
on p.month=n.month
ORDER BY p.month


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL

SELECT 
      FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d",date))
      ,SUM(totals.transactions)/COUNT(DISTINCT fullVisitorId) avg_totals_transaction_per_user 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
WHERE totals.transactions>=1
GROUP BY 1

-- Query 06: Average amount of money spent per session
#standardSQL

SELECT 
      FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d",date))
      ,(SUM(totals.totaltransactionrevenue)/ SUM(totals.visits))/1000000 avg_revenue_by_user_per_visit     
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions IS NOT NULL
GROUP BY 1
-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL

WITH sub as (
      SELECT    
            DISTINCT fullvisitorId     
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` ,
      UNNEST (hits) as hits,
      UNNEST (product) as product
      WHERE product.v2ProductName="YouTube Men's Vintage Henley"
      and totals.transactions>=1 
      AND product.productrevenue IS NOT NULL
)

SELECT  
      product.v2ProductName other_purchased_products
      ,sum(product.productquantity) quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` as b ,
UNNEST (hits) as hits,
UNNEST (product) as product
INNER JOIN sub
ON sub.fullvisitorId=b.fullvisitorId
WHERE product.productrevenue IS NOT NULL
and totals.transactions>=1
AND product.v2ProductName  NOT LIKE "YouTube Men's Vintage Henley"
GROUP BY 1
ORDER BY quantity desc


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

WITH view AS (
      SELECT 
            FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",DATE)) month
            ,COUNT(product.v2productName) num_product_view    
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`, 
      UNNEST (hits) as hits,
      UNNEST (product) as product
      WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
      AND eCommerceAction.action_type='2'
      GROUP BY 1
)
,

atc AS (
      SELECT 
            FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",DATE)) month
            ,COUNT(product.v2productName) num_add_tocart    
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`, 
      UNNEST (hits) as hits,
      UNNEST (product) as product
      WHERE  _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
      AND eCommerceAction.action_type='3'
      GROUP BY 1
)
,

purchase AS (
      SELECT 
            FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",DATE)) month
            ,COUNT(product.v2productName) num_purchase    
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`, 
      UNNEST (hits) as hits,
      UNNEST (product) as product
      WHERE  _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
      AND eCommerceAction.action_type='6'
      GROUP BY 1
)

SELECT 
       view.month
       ,num_product_view
       ,num_add_tocart
       ,num_purchase
       ,ROUND(num_add_tocart *100/num_product_view,2) add_tocart_rate 
       ROUND(num_purchase *100/num_product_view,2) purchase_rate 
FROM view 
Left JOIN atc         
ON view.month=atc.month
left JOIN purchase 
ON view.month=purchase.month
ORDER BY view.month

Cách 2: 

with product_data as(
      select
          format_date('%Y%m', parse_date('%Y%m%d',date)) as month
          ,count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view
          ,count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart
          ,count(CASE WHEN eCommerceAction.action_type = '6' THEN product.v2ProductName END) as num_purchase
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
      ,UNNEST(hits) as hits
      ,UNNEST (hits.product) as product
      where _table_suffix between '20170101' and '20170331'
      and eCommerceAction.action_type in ('2','3','6')
      group by month
      order by month
)

select
    *
    ,round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate
    ,,round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data


                                          
