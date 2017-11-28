CREATE VIEW dbo.vw_daily_price AS
SELECT price_time 
,ticker
,price_close
,stdev(price_high - price_low) OVER(PARTITION BY ticker ORDER BY price_time ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) as sfb
,stdev(price_high - price_low) OVER(PARTITION BY ticker ORDER BY price_time ROWS BETWEEN 900 PRECEDING AND CURRENT ROW) as lfb
,IIF( 
	stdev(price_high - price_low) OVER(PARTITION BY ticker ORDER BY price_time ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) > stdev(price_high - price_low) OVER(PARTITION BY ticker ORDER BY price_time ROWS BETWEEN 900 PRECEDING AND CURRENT ROW)
	,stdev(price_high - price_low) OVER(PARTITION BY ticker ORDER BY price_time ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)
	,stdev(price_high - price_low) OVER(PARTITION BY ticker ORDER BY price_time ROWS BETWEEN 900 PRECEDING AND CURRENT ROW)
	) as fb
,0.7 * IIF( 
	stdev(price_high - price_low) OVER(PARTITION BY ticker ORDER BY price_time ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) > stdev(price_high - price_low) OVER(PARTITION BY ticker ORDER BY price_time ROWS BETWEEN 900 PRECEDING AND CURRENT ROW)
	,stdev(price_high - price_low) OVER(PARTITION BY ticker ORDER BY price_time ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)
	,stdev(price_high - price_low) OVER(PARTITION BY ticker ORDER BY price_time ROWS BETWEEN 900 PRECEDING AND CURRENT ROW)
	) as hfb
FROM dbo.daily_price