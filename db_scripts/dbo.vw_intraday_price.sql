USE RLCO
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW dbo.vw_intraday_price AS
SELECT price_time
,CONVERT(date, price_time) as price_date
,ticker
,price_close
,rl30
,MAX(rl30) OVER(PARTITION BY ticker ORDER BY price_time ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) as rl30_high
,MIN(rl30) OVER(PARTITION BY ticker ORDER BY price_time ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) as rl30_low
,(MAX(rl30) OVER(PARTITION BY ticker ORDER BY price_time ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) - MIN(rl30) OVER(PARTITION BY ticker ORDER BY price_time ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)) as rl30_range
FROM dbo.intraday_price
GO


