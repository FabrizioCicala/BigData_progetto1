
add jar /home/fabrizio/Documenti/workspace/intelliJ/primoProgetto/target/fab-primoProgetto-1.0-SNAPSHOT.jar;

DROP TABLE csv;
CREATE TABLE csv (
	Id STRING,
	ProductID STRING,
	UserID STRING,
	ProfileName STRING,
	HelpNum STRING,
	HelpDenom STRING,
	Score STRING,
	Time STRING,
	Summary STRING,
	Text STRING)
	ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';
	
LOAD DATA LOCAL INPATH '/home/fabrizio/Documenti/universita/magistrale/big_data/progetto1/Reviews.csv'
	OVERWRITE INTO TABLE csv;

CREATE TEMPORARY FUNCTION unix_year AS 'hive.functions.ParseDate';

CREATE TEMPORARY TABLE filteredCsv AS
	SELECT unix_year(Time) AS year, ProductID, Score
	FROM csv;

CREATE TEMPORARY TABLE job2_result AS
	SELECT ProductID, year, AVG(Score) AS average
	FROM filteredCsv
	WHERE year >= 2003 AND year <= 2012
	GROUP BY ProductID, year
	ORDER BY ProductID, year;

SELECT * FROM job2_result;





