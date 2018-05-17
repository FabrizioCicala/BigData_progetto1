
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

CREATE TEMPORARY TABLE year2summary AS
	SELECT year(from_unixtime(Time)) AS year, Summary
	FROM csv;

CREATE TEMPORARY TABLE used_words AS
	SELECT year, word, count(1) AS num
	FROM year2summary
	LATERAL VIEW explode(split(trim(regexp_replace(lower(Summary), '[^A-Za-z0-9]', ' ')), ' +'))words AS word
	GROUP BY year, word;

CREATE TEMPORARY TABLE sorted_rows AS
	SELECT year, word, num, row_number() over (PARTITION BY year ORDER BY num DESC) AS rank
	FROM used_words;

CREATE TABLE job1_result AS
	SELECT year, word, num
	FROM sorted_rows
	WHERE rank<=10 and year!='NULL';

--SELECT * from job1_result;