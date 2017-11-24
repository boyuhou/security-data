USE RLCO
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID('dbo.intraday_price', 'U') IS NOT NULL
	DROP TABLE dbo.intraday_price

CREATE TABLE [dbo].[intraday_price](
	price_time smalldatetime NOT NULL,
	ticker varchar(10) NOT NULL,
	price_open money NOT NULL,
	price_high money NOT NULL,
	price_low money NOT NULL,
	price_close money NOT NULL,
	rl10 money NOT NULL,
	rl30 money NOT NULL,
) ON [PRIMARY]

ALTER TABLE dbo.intraday_price ADD CONSTRAINT pk_intraday_price PRIMARY KEY (price_time, ticker)
GO


