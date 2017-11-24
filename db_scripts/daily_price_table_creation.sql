USE  RLCO
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID('dbo.daily_price', 'U') IS NOT NULL
	DROP TABLE dbo.daily_price

CREATE TABLE dbo.daily_price (
	 price_time date NOT NULL,
	 ticker varchar(10) NOT NULL,
	 price_open money NOT NULL,
	 price_high money NOT NULL,
	 price_low money NOT NULL,
	 price_close money NOT NULL,
	 rl10 money NOT NULL,
	 rl30 money NOT NULL
) ON [PRIMARY]

ALTER TABLE dbo.daily_price ADD CONSTRAINT pk_daily_price PRIMARY KEY (price_time, ticker)
GO


