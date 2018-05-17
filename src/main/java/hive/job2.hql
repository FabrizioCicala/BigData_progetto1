
CREATE TABLE if not exists csv (
	Id STRING,
	ProductID STRING,
	UserID STRING,
	ProfileName STRING,
	HelpNum STRING,
	HelpDenom STRING,
	Score STRING,
	Time BIGINT,
	Summary STRING,
	Text STRING)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
	
LOAD DATA LOCAL INPATH '/home/fabrizio/Documenti/universita/magistrale/big_data/progetto1/Reviews.csv'
	OVERWRITE INTO TABLE csv;

CREATE TEMPORARY TABLE filteredCsv AS
	SELECT year(from_unixtime(Time)) AS year, ProductID, Score
	FROM csv;

CREATE TABLE job2_result AS
	SELECT ProductID, year(from_unixtime(Time)) AS year, round(AVG(Score), 2) AS average
	FROM filteredCsv
	WHERE year >= 2003 AND year <= 2012
	GROUP BY ProductID, year
	ORDER BY ProductID, year;

--SELECT * FROM job2_result;





