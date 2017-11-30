USE [RLCO]
GO

/****** Object:  StoredProcedure [dbo].[find_intraday_owl]    Script Date: 29/11/2017 7:46:32 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


ALTER PROCEDURE [dbo].[find_intraday_owl]
	@dtDate DATE
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @dtIntradayLookback DATE
	DECLARE @dtDailyLookback DATE
	DECLARE @dtLookForward DATE

	SELECT @dtIntradayLookback = DATEADD(day, -5, @dtDate)
		,@dtDailyLookback = DATEADD(year, -5, @dtDate)
		,@dtLookForward = DATEADD(day, 1, @dtDate)

	IF OBJECT_ID('tempdb..#intraday') IS NOT NULL DROP TABLE #intraday;
	IF OBJECT_ID('tempdb..#daily') IS NOT NULL DROP TABLE #daily;

	CREATE TABLE #intraday
	(
		price_time DATETIME NOT NULL,
		price_date DATE NOT NULL,
		ticker VARCHAR(10) NOT NULL,
		price_close money NOT NULL,
		rl30 money NOT NULL,
		rl30_high money NOT NULL,
		rl30_low money NOT NULL,
		rl30_range money NOT NULL
	)
	ALTER TABLE #intraday ADD CONSTRAINT pk_intraday PRIMARY KEY(price_time, price_date, ticker)

	CREATE TABLE #daily
	(
		 price_time date NOT NULL,
		 ticker varchar(10) NOT NULL,
		 price_close money NOT NULL,
		 sfb money NULL,
		 lfb money NULL,
		 fb money NULL,
		 hfb money NULL
	)
	ALTER TABLE #daily ADD CONSTRAINT pk_daily PRIMARY KEY (price_time, ticker)

	INSERT INTO #intraday
	SELECT price_time
		,CONVERT(date, price_time) as price_date
		,ticker
		,price_close
		,rl30
		,MAX(rl30) OVER(PARTITION BY ticker ORDER BY price_time ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) as rl30_high
		,MIN(rl30) OVER(PARTITION BY ticker ORDER BY price_time ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) as rl30_low
		,(MAX(rl30) OVER(PARTITION BY ticker ORDER BY price_time ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) - MIN(rl30) OVER(PARTITION BY ticker ORDER BY price_time ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)) as rl30_range
	FROM dbo.intraday_price
	WHERE price_time BETWEEN @dtIntradayLookback AND @dtLookForward

	INSERT INTO #daily
	SELECT price_time, ticker, price_close, sfb, lfb, fb, hfb
	FROM (
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
	WHERE price_time BETWEEN @dtDailyLookback AND @dtLookForward
	) AS e
	WHERE price_time = @dtDate

	--SELECT RIGHT(CONVERT(VARCHAR, i.price_time, 120), 8) AS intraday_time
	--	,i.ticker
	--	,IIF(i.rl30_range > 0.5 *fb, 1, 0) as frogowl
	--FROM #intraday i
	--JOIN #daily d
	--ON d.price_time = i.price_date
	--and d.ticker = i.ticker

	SELECT ticker,
	ISNULL([09:31:00],0) AS [09:31:00],
			ISNULL([09:32:00],0) AS [09:32:00],
			ISNULL([09:33:00],0) AS [09:33:00],
			ISNULL([09:34:00],0) AS [09:34:00],
			ISNULL([09:35:00],0) AS [09:35:00],
			ISNULL([09:36:00],0) AS [09:36:00],
			ISNULL([09:37:00],0) AS [09:37:00],
			ISNULL([09:38:00],0) AS [09:38:00],
			ISNULL([09:39:00],0) AS [09:39:00],
			ISNULL([09:40:00],0) AS [09:40:00],
			ISNULL([09:41:00],0) AS [09:41:00],
			ISNULL([09:42:00],0) AS [09:42:00],
			ISNULL([09:43:00],0) AS [09:43:00],
			ISNULL([09:44:00],0) AS [09:44:00],
			ISNULL([09:45:00],0) AS [09:45:00],
			ISNULL([09:46:00],0) AS [09:46:00],
			ISNULL([09:47:00],0) AS [09:47:00],
			ISNULL([09:48:00],0) AS [09:48:00],
			ISNULL([09:49:00],0) AS [09:49:00],
			ISNULL([09:50:00],0) AS [09:50:00],
			ISNULL([09:51:00],0) AS [09:51:00],
			ISNULL([09:52:00],0) AS [09:52:00],
			ISNULL([09:53:00],0) AS [09:53:00],
			ISNULL([09:54:00],0) AS [09:54:00],
			ISNULL([09:55:00],0) AS [09:55:00],
			ISNULL([09:56:00],0) AS [09:56:00],
			ISNULL([09:57:00],0) AS [09:57:00],
			ISNULL([09:58:00],0) AS [09:58:00],
			ISNULL([09:59:00],0) AS [09:59:00],
			ISNULL([10:00:00],0) AS [10:00:00],
			ISNULL([10:01:00],0) AS [10:01:00],
			ISNULL([10:02:00],0) AS [10:02:00],
			ISNULL([10:03:00],0) AS [10:03:00],
			ISNULL([10:04:00],0) AS [10:04:00],
			ISNULL([10:05:00],0) AS [10:05:00],
			ISNULL([10:06:00],0) AS [10:06:00],
			ISNULL([10:07:00],0) AS [10:07:00],
			ISNULL([10:08:00],0) AS [10:08:00],
			ISNULL([10:09:00],0) AS [10:09:00],
			ISNULL([10:10:00],0) AS [10:10:00],
			ISNULL([10:11:00],0) AS [10:11:00],
			ISNULL([10:12:00],0) AS [10:12:00],
			ISNULL([10:13:00],0) AS [10:13:00],
			ISNULL([10:14:00],0) AS [10:14:00],
			ISNULL([10:15:00],0) AS [10:15:00],
			ISNULL([10:16:00],0) AS [10:16:00],
			ISNULL([10:17:00],0) AS [10:17:00],
			ISNULL([10:18:00],0) AS [10:18:00],
			ISNULL([10:19:00],0) AS [10:19:00],
			ISNULL([10:20:00],0) AS [10:20:00],
			ISNULL([10:21:00],0) AS [10:21:00],
			ISNULL([10:22:00],0) AS [10:22:00],
			ISNULL([10:23:00],0) AS [10:23:00],
			ISNULL([10:24:00],0) AS [10:24:00],
			ISNULL([10:25:00],0) AS [10:25:00],
			ISNULL([10:26:00],0) AS [10:26:00],
			ISNULL([10:27:00],0) AS [10:27:00],
			ISNULL([10:28:00],0) AS [10:28:00],
			ISNULL([10:29:00],0) AS [10:29:00],
			ISNULL([10:30:00],0) AS [10:30:00],
			ISNULL([10:31:00],0) AS [10:31:00],
			ISNULL([10:32:00],0) AS [10:32:00],
			ISNULL([10:33:00],0) AS [10:33:00],
			ISNULL([10:34:00],0) AS [10:34:00],
			ISNULL([10:35:00],0) AS [10:35:00],
			ISNULL([10:36:00],0) AS [10:36:00],
			ISNULL([10:37:00],0) AS [10:37:00],
			ISNULL([10:38:00],0) AS [10:38:00],
			ISNULL([10:39:00],0) AS [10:39:00],
			ISNULL([10:40:00],0) AS [10:40:00],
			ISNULL([10:41:00],0) AS [10:41:00],
			ISNULL([10:42:00],0) AS [10:42:00],
			ISNULL([10:43:00],0) AS [10:43:00],
			ISNULL([10:44:00],0) AS [10:44:00],
			ISNULL([10:45:00],0) AS [10:45:00],
			ISNULL([10:46:00],0) AS [10:46:00],
			ISNULL([10:47:00],0) AS [10:47:00],
			ISNULL([10:48:00],0) AS [10:48:00],
			ISNULL([10:49:00],0) AS [10:49:00],
			ISNULL([10:50:00],0) AS [10:50:00],
			ISNULL([10:51:00],0) AS [10:51:00],
			ISNULL([10:52:00],0) AS [10:52:00],
			ISNULL([10:53:00],0) AS [10:53:00],
			ISNULL([10:54:00],0) AS [10:54:00],
			ISNULL([10:55:00],0) AS [10:55:00],
			ISNULL([10:56:00],0) AS [10:56:00],
			ISNULL([10:57:00],0) AS [10:57:00],
			ISNULL([10:58:00],0) AS [10:58:00],
			ISNULL([10:59:00],0) AS [10:59:00],
			ISNULL([11:00:00],0) AS [11:00:00],
			ISNULL([11:01:00],0) AS [11:01:00],
			ISNULL([11:02:00],0) AS [11:02:00],
			ISNULL([11:03:00],0) AS [11:03:00],
			ISNULL([11:04:00],0) AS [11:04:00],
			ISNULL([11:05:00],0) AS [11:05:00],
			ISNULL([11:06:00],0) AS [11:06:00],
			ISNULL([11:07:00],0) AS [11:07:00],
			ISNULL([11:08:00],0) AS [11:08:00],
			ISNULL([11:09:00],0) AS [11:09:00],
			ISNULL([11:10:00],0) AS [11:10:00],
			ISNULL([11:11:00],0) AS [11:11:00],
			ISNULL([11:12:00],0) AS [11:12:00],
			ISNULL([11:13:00],0) AS [11:13:00],
			ISNULL([11:14:00],0) AS [11:14:00],
			ISNULL([11:15:00],0) AS [11:15:00],
			ISNULL([11:16:00],0) AS [11:16:00],
			ISNULL([11:17:00],0) AS [11:17:00],
			ISNULL([11:18:00],0) AS [11:18:00],
			ISNULL([11:19:00],0) AS [11:19:00],
			ISNULL([11:20:00],0) AS [11:20:00],
			ISNULL([11:21:00],0) AS [11:21:00],
			ISNULL([11:22:00],0) AS [11:22:00],
			ISNULL([11:23:00],0) AS [11:23:00],
			ISNULL([11:24:00],0) AS [11:24:00],
			ISNULL([11:25:00],0) AS [11:25:00],
			ISNULL([11:26:00],0) AS [11:26:00],
			ISNULL([11:27:00],0) AS [11:27:00],
			ISNULL([11:28:00],0) AS [11:28:00],
			ISNULL([11:29:00],0) AS [11:29:00],
			ISNULL([11:30:00],0) AS [11:30:00],
			ISNULL([11:31:00],0) AS [11:31:00],
			ISNULL([11:32:00],0) AS [11:32:00],
			ISNULL([11:33:00],0) AS [11:33:00],
			ISNULL([11:34:00],0) AS [11:34:00],
			ISNULL([11:35:00],0) AS [11:35:00],
			ISNULL([11:36:00],0) AS [11:36:00],
			ISNULL([11:37:00],0) AS [11:37:00],
			ISNULL([11:38:00],0) AS [11:38:00],
			ISNULL([11:39:00],0) AS [11:39:00],
			ISNULL([11:40:00],0) AS [11:40:00],
			ISNULL([11:41:00],0) AS [11:41:00],
			ISNULL([11:42:00],0) AS [11:42:00],
			ISNULL([11:43:00],0) AS [11:43:00],
			ISNULL([11:44:00],0) AS [11:44:00],
			ISNULL([11:45:00],0) AS [11:45:00],
			ISNULL([11:46:00],0) AS [11:46:00],
			ISNULL([11:47:00],0) AS [11:47:00],
			ISNULL([11:48:00],0) AS [11:48:00],
			ISNULL([11:49:00],0) AS [11:49:00],
			ISNULL([11:50:00],0) AS [11:50:00],
			ISNULL([11:51:00],0) AS [11:51:00],
			ISNULL([11:52:00],0) AS [11:52:00],
			ISNULL([11:53:00],0) AS [11:53:00],
			ISNULL([11:54:00],0) AS [11:54:00],
			ISNULL([11:55:00],0) AS [11:55:00],
			ISNULL([11:56:00],0) AS [11:56:00],
			ISNULL([11:57:00],0) AS [11:57:00],
			ISNULL([11:58:00],0) AS [11:58:00],
			ISNULL([11:59:00],0) AS [11:59:00],
			ISNULL([12:00:00],0) AS [12:00:00],
			ISNULL([12:01:00],0) AS [12:01:00],
			ISNULL([12:02:00],0) AS [12:02:00],
			ISNULL([12:03:00],0) AS [12:03:00],
			ISNULL([12:04:00],0) AS [12:04:00],
			ISNULL([12:05:00],0) AS [12:05:00],
			ISNULL([12:06:00],0) AS [12:06:00],
			ISNULL([12:07:00],0) AS [12:07:00],
			ISNULL([12:08:00],0) AS [12:08:00],
			ISNULL([12:09:00],0) AS [12:09:00],
			ISNULL([12:10:00],0) AS [12:10:00],
			ISNULL([12:11:00],0) AS [12:11:00],
			ISNULL([12:12:00],0) AS [12:12:00],
			ISNULL([12:13:00],0) AS [12:13:00],
			ISNULL([12:14:00],0) AS [12:14:00],
			ISNULL([12:15:00],0) AS [12:15:00],
			ISNULL([12:16:00],0) AS [12:16:00],
			ISNULL([12:17:00],0) AS [12:17:00],
			ISNULL([12:18:00],0) AS [12:18:00],
			ISNULL([12:19:00],0) AS [12:19:00],
			ISNULL([12:20:00],0) AS [12:20:00],
			ISNULL([12:21:00],0) AS [12:21:00],
			ISNULL([12:22:00],0) AS [12:22:00],
			ISNULL([12:23:00],0) AS [12:23:00],
			ISNULL([12:24:00],0) AS [12:24:00],
			ISNULL([12:25:00],0) AS [12:25:00],
			ISNULL([12:26:00],0) AS [12:26:00],
			ISNULL([12:27:00],0) AS [12:27:00],
			ISNULL([12:28:00],0) AS [12:28:00],
			ISNULL([12:29:00],0) AS [12:29:00],
			ISNULL([12:30:00],0) AS [12:30:00],
			ISNULL([12:31:00],0) AS [12:31:00],
			ISNULL([12:32:00],0) AS [12:32:00],
			ISNULL([12:33:00],0) AS [12:33:00],
			ISNULL([12:34:00],0) AS [12:34:00],
			ISNULL([12:35:00],0) AS [12:35:00],
			ISNULL([12:36:00],0) AS [12:36:00],
			ISNULL([12:37:00],0) AS [12:37:00],
			ISNULL([12:38:00],0) AS [12:38:00],
			ISNULL([12:39:00],0) AS [12:39:00],
			ISNULL([12:40:00],0) AS [12:40:00],
			ISNULL([12:41:00],0) AS [12:41:00],
			ISNULL([12:42:00],0) AS [12:42:00],
			ISNULL([12:43:00],0) AS [12:43:00],
			ISNULL([12:44:00],0) AS [12:44:00],
			ISNULL([12:45:00],0) AS [12:45:00],
			ISNULL([12:46:00],0) AS [12:46:00],
			ISNULL([12:47:00],0) AS [12:47:00],
			ISNULL([12:48:00],0) AS [12:48:00],
			ISNULL([12:49:00],0) AS [12:49:00],
			ISNULL([12:50:00],0) AS [12:50:00],
			ISNULL([12:51:00],0) AS [12:51:00],
			ISNULL([12:52:00],0) AS [12:52:00],
			ISNULL([12:53:00],0) AS [12:53:00],
			ISNULL([12:54:00],0) AS [12:54:00],
			ISNULL([12:55:00],0) AS [12:55:00],
			ISNULL([12:56:00],0) AS [12:56:00],
			ISNULL([12:57:00],0) AS [12:57:00],
			ISNULL([12:58:00],0) AS [12:58:00],
			ISNULL([12:59:00],0) AS [12:59:00],
			ISNULL([13:00:00],0) AS [13:00:00],
			ISNULL([13:01:00],0) AS [13:01:00],
			ISNULL([13:02:00],0) AS [13:02:00],
			ISNULL([13:03:00],0) AS [13:03:00],
			ISNULL([13:04:00],0) AS [13:04:00],
			ISNULL([13:05:00],0) AS [13:05:00],
			ISNULL([13:06:00],0) AS [13:06:00],
			ISNULL([13:07:00],0) AS [13:07:00],
			ISNULL([13:08:00],0) AS [13:08:00],
			ISNULL([13:09:00],0) AS [13:09:00],
			ISNULL([13:10:00],0) AS [13:10:00],
			ISNULL([13:11:00],0) AS [13:11:00],
			ISNULL([13:12:00],0) AS [13:12:00],
			ISNULL([13:13:00],0) AS [13:13:00],
			ISNULL([13:14:00],0) AS [13:14:00],
			ISNULL([13:15:00],0) AS [13:15:00],
			ISNULL([13:16:00],0) AS [13:16:00],
			ISNULL([13:17:00],0) AS [13:17:00],
			ISNULL([13:18:00],0) AS [13:18:00],
			ISNULL([13:19:00],0) AS [13:19:00],
			ISNULL([13:20:00],0) AS [13:20:00],
			ISNULL([13:21:00],0) AS [13:21:00],
			ISNULL([13:22:00],0) AS [13:22:00],
			ISNULL([13:23:00],0) AS [13:23:00],
			ISNULL([13:24:00],0) AS [13:24:00],
			ISNULL([13:25:00],0) AS [13:25:00],
			ISNULL([13:26:00],0) AS [13:26:00],
			ISNULL([13:27:00],0) AS [13:27:00],
			ISNULL([13:28:00],0) AS [13:28:00],
			ISNULL([13:29:00],0) AS [13:29:00],
			ISNULL([13:30:00],0) AS [13:30:00],
			ISNULL([13:31:00],0) AS [13:31:00],
			ISNULL([13:32:00],0) AS [13:32:00],
			ISNULL([13:33:00],0) AS [13:33:00],
			ISNULL([13:34:00],0) AS [13:34:00],
			ISNULL([13:35:00],0) AS [13:35:00],
			ISNULL([13:36:00],0) AS [13:36:00],
			ISNULL([13:37:00],0) AS [13:37:00],
			ISNULL([13:38:00],0) AS [13:38:00],
			ISNULL([13:39:00],0) AS [13:39:00],
			ISNULL([13:40:00],0) AS [13:40:00],
			ISNULL([13:41:00],0) AS [13:41:00],
			ISNULL([13:42:00],0) AS [13:42:00],
			ISNULL([13:43:00],0) AS [13:43:00],
			ISNULL([13:44:00],0) AS [13:44:00],
			ISNULL([13:45:00],0) AS [13:45:00],
			ISNULL([13:46:00],0) AS [13:46:00],
			ISNULL([13:47:00],0) AS [13:47:00],
			ISNULL([13:48:00],0) AS [13:48:00],
			ISNULL([13:49:00],0) AS [13:49:00],
			ISNULL([13:50:00],0) AS [13:50:00],
			ISNULL([13:51:00],0) AS [13:51:00],
			ISNULL([13:52:00],0) AS [13:52:00],
			ISNULL([13:53:00],0) AS [13:53:00],
			ISNULL([13:54:00],0) AS [13:54:00],
			ISNULL([13:55:00],0) AS [13:55:00],
			ISNULL([13:56:00],0) AS [13:56:00],
			ISNULL([13:57:00],0) AS [13:57:00],
			ISNULL([13:58:00],0) AS [13:58:00],
			ISNULL([13:59:00],0) AS [13:59:00],
			ISNULL([14:00:00],0) AS [14:00:00],
			ISNULL([14:01:00],0) AS [14:01:00],
			ISNULL([14:02:00],0) AS [14:02:00],
			ISNULL([14:03:00],0) AS [14:03:00],
			ISNULL([14:04:00],0) AS [14:04:00],
			ISNULL([14:05:00],0) AS [14:05:00],
			ISNULL([14:06:00],0) AS [14:06:00],
			ISNULL([14:07:00],0) AS [14:07:00],
			ISNULL([14:08:00],0) AS [14:08:00],
			ISNULL([14:09:00],0) AS [14:09:00],
			ISNULL([14:10:00],0) AS [14:10:00],
			ISNULL([14:11:00],0) AS [14:11:00],
			ISNULL([14:12:00],0) AS [14:12:00],
			ISNULL([14:13:00],0) AS [14:13:00],
			ISNULL([14:14:00],0) AS [14:14:00],
			ISNULL([14:15:00],0) AS [14:15:00],
			ISNULL([14:16:00],0) AS [14:16:00],
			ISNULL([14:17:00],0) AS [14:17:00],
			ISNULL([14:18:00],0) AS [14:18:00],
			ISNULL([14:19:00],0) AS [14:19:00],
			ISNULL([14:20:00],0) AS [14:20:00],
			ISNULL([14:21:00],0) AS [14:21:00],
			ISNULL([14:22:00],0) AS [14:22:00],
			ISNULL([14:23:00],0) AS [14:23:00],
			ISNULL([14:24:00],0) AS [14:24:00],
			ISNULL([14:25:00],0) AS [14:25:00],
			ISNULL([14:26:00],0) AS [14:26:00],
			ISNULL([14:27:00],0) AS [14:27:00],
			ISNULL([14:28:00],0) AS [14:28:00],
			ISNULL([14:29:00],0) AS [14:29:00],
			ISNULL([14:30:00],0) AS [14:30:00],
			ISNULL([14:31:00],0) AS [14:31:00],
			ISNULL([14:32:00],0) AS [14:32:00],
			ISNULL([14:33:00],0) AS [14:33:00],
			ISNULL([14:34:00],0) AS [14:34:00],
			ISNULL([14:35:00],0) AS [14:35:00],
			ISNULL([14:36:00],0) AS [14:36:00],
			ISNULL([14:37:00],0) AS [14:37:00],
			ISNULL([14:38:00],0) AS [14:38:00],
			ISNULL([14:39:00],0) AS [14:39:00],
			ISNULL([14:40:00],0) AS [14:40:00],
			ISNULL([14:41:00],0) AS [14:41:00],
			ISNULL([14:42:00],0) AS [14:42:00],
			ISNULL([14:43:00],0) AS [14:43:00],
			ISNULL([14:44:00],0) AS [14:44:00],
			ISNULL([14:45:00],0) AS [14:45:00],
			ISNULL([14:46:00],0) AS [14:46:00],
			ISNULL([14:47:00],0) AS [14:47:00],
			ISNULL([14:48:00],0) AS [14:48:00],
			ISNULL([14:49:00],0) AS [14:49:00],
			ISNULL([14:50:00],0) AS [14:50:00],
			ISNULL([14:51:00],0) AS [14:51:00],
			ISNULL([14:52:00],0) AS [14:52:00],
			ISNULL([14:53:00],0) AS [14:53:00],
			ISNULL([14:54:00],0) AS [14:54:00],
			ISNULL([14:55:00],0) AS [14:55:00],
			ISNULL([14:56:00],0) AS [14:56:00],
			ISNULL([14:57:00],0) AS [14:57:00],
			ISNULL([14:58:00],0) AS [14:58:00],
			ISNULL([14:59:00],0) AS [14:59:00],
			ISNULL([15:00:00],0) AS [15:00:00],
			ISNULL([15:01:00],0) AS [15:01:00],
			ISNULL([15:02:00],0) AS [15:02:00],
			ISNULL([15:03:00],0) AS [15:03:00],
			ISNULL([15:04:00],0) AS [15:04:00],
			ISNULL([15:05:00],0) AS [15:05:00],
			ISNULL([15:06:00],0) AS [15:06:00],
			ISNULL([15:07:00],0) AS [15:07:00],
			ISNULL([15:08:00],0) AS [15:08:00],
			ISNULL([15:09:00],0) AS [15:09:00],
			ISNULL([15:10:00],0) AS [15:10:00],
			ISNULL([15:11:00],0) AS [15:11:00],
			ISNULL([15:12:00],0) AS [15:12:00],
			ISNULL([15:13:00],0) AS [15:13:00],
			ISNULL([15:14:00],0) AS [15:14:00],
			ISNULL([15:15:00],0) AS [15:15:00],
			ISNULL([15:16:00],0) AS [15:16:00],
			ISNULL([15:17:00],0) AS [15:17:00],
			ISNULL([15:18:00],0) AS [15:18:00],
			ISNULL([15:19:00],0) AS [15:19:00],
			ISNULL([15:20:00],0) AS [15:20:00],
			ISNULL([15:21:00],0) AS [15:21:00],
			ISNULL([15:22:00],0) AS [15:22:00],
			ISNULL([15:23:00],0) AS [15:23:00],
			ISNULL([15:24:00],0) AS [15:24:00],
			ISNULL([15:25:00],0) AS [15:25:00],
			ISNULL([15:26:00],0) AS [15:26:00],
			ISNULL([15:27:00],0) AS [15:27:00],
			ISNULL([15:28:00],0) AS [15:28:00],
			ISNULL([15:29:00],0) AS [15:29:00],
			ISNULL([15:30:00],0) AS [15:30:00],
			ISNULL([15:31:00],0) AS [15:31:00],
			ISNULL([15:32:00],0) AS [15:32:00],
			ISNULL([15:33:00],0) AS [15:33:00],
			ISNULL([15:34:00],0) AS [15:34:00],
			ISNULL([15:35:00],0) AS [15:35:00],
			ISNULL([15:36:00],0) AS [15:36:00],
			ISNULL([15:37:00],0) AS [15:37:00],
			ISNULL([15:38:00],0) AS [15:38:00],
			ISNULL([15:39:00],0) AS [15:39:00],
			ISNULL([15:40:00],0) AS [15:40:00],
			ISNULL([15:41:00],0) AS [15:41:00],
			ISNULL([15:42:00],0) AS [15:42:00],
			ISNULL([15:43:00],0) AS [15:43:00],
			ISNULL([15:44:00],0) AS [15:44:00],
			ISNULL([15:45:00],0) AS [15:45:00],
			ISNULL([15:46:00],0) AS [15:46:00],
			ISNULL([15:47:00],0) AS [15:47:00],
			ISNULL([15:48:00],0) AS [15:48:00],
			ISNULL([15:49:00],0) AS [15:49:00],
			ISNULL([15:50:00],0) AS [15:50:00],
			ISNULL([15:51:00],0) AS [15:51:00],
			ISNULL([15:52:00],0) AS [15:52:00],
			ISNULL([15:53:00],0) AS [15:53:00],
			ISNULL([15:54:00],0) AS [15:54:00],
			ISNULL([15:55:00],0) AS [15:55:00],
			ISNULL([15:56:00],0) AS [15:56:00],
			ISNULL([15:57:00],0) AS [15:57:00],
			ISNULL([15:58:00],0) AS [15:58:00],
			ISNULL([15:59:00],0) AS [15:59:00],
			ISNULL([16:00:00],0) AS [16:00:00]
	FROM
	(
		SELECT RIGHT(CONVERT(VARCHAR, i.price_time, 120), 8) AS intraday_time
			,i.ticker as ticker
			,IIF(i.rl30_range > 0.5 *fb, 1, 0) as frogowl
		FROM #intraday i
		JOIN #daily d
		ON d.price_time = i.price_date
		and d.ticker = i.ticker
	) x
	pivot
	(
		max(frogowl)
		for intraday_time in ([09:31:00], [09:32:00], [09:33:00], [09:34:00], [09:35:00], [09:36:00], [09:37:00], [09:38:00], [09:39:00], [09:40:00], [09:41:00], [09:42:00], [09:43:00], [09:44:00], [09:45:00], [09:46:00], [09:47:00], [09:48:00], [09:49:00], [09:50:00], [09:51:00], [09:52:00], [09:53:00], [09:54:00], [09:55:00], [09:56:00], [09:57:00], [09:58:00], [09:59:00], [10:00:00], [10:01:00], [10:02:00], [10:03:00], [10:04:00], [10:05:00], [10:06:00], [10:07:00], [10:08:00], [10:09:00], [10:10:00], [10:11:00], [10:12:00], [10:13:00], [10:14:00], [10:15:00], [10:16:00], [10:17:00], [10:18:00], [10:19:00], [10:20:00], [10:21:00], [10:22:00], [10:23:00], [10:24:00], [10:25:00], [10:26:00], [10:27:00], [10:28:00], [10:29:00], [10:30:00], [10:31:00], [10:32:00], [10:33:00], [10:34:00], [10:35:00], [10:36:00], [10:37:00], [10:38:00], [10:39:00], [10:40:00], [10:41:00], [10:42:00], [10:43:00], [10:44:00], [10:45:00], [10:46:00], [10:47:00], [10:48:00], [10:49:00], [10:50:00], [10:51:00], [10:52:00], [10:53:00], [10:54:00], [10:55:00], [10:56:00], [10:57:00], [10:58:00], [10:59:00], [11:00:00], [11:01:00], [11:02:00], [11:03:00], [11:04:00], [11:05:00], [11:06:00], [11:07:00], [11:08:00], [11:09:00], [11:10:00], [11:11:00], [11:12:00], [11:13:00], [11:14:00], [11:15:00], [11:16:00], [11:17:00], [11:18:00], [11:19:00], [11:20:00], [11:21:00], [11:22:00], [11:23:00], [11:24:00], [11:25:00], [11:26:00], [11:27:00], [11:28:00], [11:29:00], [11:30:00], [11:31:00], [11:32:00], [11:33:00], [11:34:00], [11:35:00], [11:36:00], [11:37:00], [11:38:00], [11:39:00], [11:40:00], [11:41:00], [11:42:00], [11:43:00], [11:44:00], [11:45:00], [11:46:00], [11:47:00], [11:48:00], [11:49:00], [11:50:00], [11:51:00], [11:52:00], [11:53:00], [11:54:00], [11:55:00], [11:56:00], [11:57:00], [11:58:00], [11:59:00], [12:00:00], [12:01:00], [12:02:00], [12:03:00], [12:04:00], [12:05:00], [12:06:00], [12:07:00], [12:08:00], [12:09:00], [12:10:00], [12:11:00], [12:12:00], [12:13:00], [12:14:00], [12:15:00], [12:16:00], [12:17:00], [12:18:00], [12:19:00], [12:20:00], [12:21:00], [12:22:00], [12:23:00], [12:24:00], [12:25:00], [12:26:00], [12:27:00], [12:28:00], [12:29:00], [12:30:00], [12:31:00], [12:32:00], [12:33:00], [12:34:00], [12:35:00], [12:36:00], [12:37:00], [12:38:00], [12:39:00], [12:40:00], [12:41:00], [12:42:00], [12:43:00], [12:44:00], [12:45:00], [12:46:00], [12:47:00], [12:48:00], [12:49:00], [12:50:00], [12:51:00], [12:52:00], [12:53:00], [12:54:00], [12:55:00], [12:56:00], [12:57:00], [12:58:00], [12:59:00], [13:00:00], [13:01:00], [13:02:00], [13:03:00], [13:04:00], [13:05:00], [13:06:00], [13:07:00], [13:08:00], [13:09:00], [13:10:00], [13:11:00], [13:12:00], [13:13:00], [13:14:00], [13:15:00], [13:16:00], [13:17:00], [13:18:00], [13:19:00], [13:20:00], [13:21:00], [13:22:00], [13:23:00], [13:24:00], [13:25:00], [13:26:00], [13:27:00], [13:28:00], [13:29:00], [13:30:00], [13:31:00], [13:32:00], [13:33:00], [13:34:00], [13:35:00], [13:36:00], [13:37:00], [13:38:00], [13:39:00], [13:40:00], [13:41:00], [13:42:00], [13:43:00], [13:44:00], [13:45:00], [13:46:00], [13:47:00], [13:48:00], [13:49:00], [13:50:00], [13:51:00], [13:52:00], [13:53:00], [13:54:00], [13:55:00], [13:56:00], [13:57:00], [13:58:00], [13:59:00], [14:00:00], [14:01:00], [14:02:00], [14:03:00], [14:04:00], [14:05:00], [14:06:00], [14:07:00], [14:08:00], [14:09:00], [14:10:00], [14:11:00], [14:12:00], [14:13:00], [14:14:00], [14:15:00], [14:16:00], [14:17:00], [14:18:00], [14:19:00], [14:20:00], [14:21:00], [14:22:00], [14:23:00], [14:24:00], [14:25:00], [14:26:00], [14:27:00], [14:28:00], [14:29:00], [14:30:00], [14:31:00], [14:32:00], [14:33:00], [14:34:00], [14:35:00], [14:36:00], [14:37:00], [14:38:00], [14:39:00], [14:40:00], [14:41:00], [14:42:00], [14:43:00], [14:44:00], [14:45:00], [14:46:00], [14:47:00], [14:48:00], [14:49:00], [14:50:00], [14:51:00], [14:52:00], [14:53:00], [14:54:00], [14:55:00], [14:56:00], [14:57:00], [14:58:00], [14:59:00], [15:00:00], [15:01:00], [15:02:00], [15:03:00], [15:04:00], [15:05:00], [15:06:00], [15:07:00], [15:08:00], [15:09:00], [15:10:00], [15:11:00], [15:12:00], [15:13:00], [15:14:00], [15:15:00], [15:16:00], [15:17:00], [15:18:00], [15:19:00], [15:20:00], [15:21:00], [15:22:00], [15:23:00], [15:24:00], [15:25:00], [15:26:00], [15:27:00], [15:28:00], [15:29:00], [15:30:00], [15:31:00], [15:32:00], [15:33:00], [15:34:00], [15:35:00], [15:36:00], [15:37:00], [15:38:00], [15:39:00], [15:40:00], [15:41:00], [15:42:00], [15:43:00], [15:44:00], [15:45:00], [15:46:00], [15:47:00], [15:48:00], [15:49:00], [15:50:00], [15:51:00], [15:52:00], [15:53:00], [15:54:00], [15:55:00], [15:56:00], [15:57:00], [15:58:00], [15:59:00], [16:00:00])
	)p


	IF OBJECT_ID('tempdb..#intraday') IS NOT NULL DROP TABLE #intraday;
	IF OBJECT_ID('tempdb..#daily') IS NOT NULL DROP TABLE #daily;

END


GO


