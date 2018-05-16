
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

CREATE TEMPORARY TABLE prod2user AS
    SELECT ProductID, UserID
    FROM csv;

CREATE TEMPORARY TABLE couples AS
    SELECT  t1.ProductID AS prod1, t2.ProductID AS prod2
    FROM prod2user t1 JOIN prod2user t2 ON t1.UserID=t2.UserID
    WHERE t1.ProductID<t2.ProductID;

CREATE TEMPORARY TABLE job3_result AS
    SELECT prod1, prod2, count(1) AS count
    FROM couples
    GROUP BY prod1, prod2
    ORDER BY prod1, prod2;

SELECT * FROM job3_result;