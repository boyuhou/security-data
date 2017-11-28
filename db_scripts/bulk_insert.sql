BULK INSERT RLCO.dbo.intraday_price
FROM 'C:\\bhougithub\\security-data\\intraday.csv' WITH (
    FIELDTERMINATOR=',',
    ROWTERMINATOR='\n'
);

BULK INSERT RLCO.dbo.daily_price
FROM 'C:\\bhougithub\\security-data\\daily.csv' WITH (
	FIELDTERMINATOR=',',
	ROWTERMINATOR='\n'
);      