
add jar /home/fabrizio/Documenti/workspace/intelliJ/primoProgetto/target/fab-primoProgetto-1.0-SNAPSHOT.jar;

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
	
CREATE TEMPORARY FUNCTION unix_year AS 'hive.job1.ParseDate';
CREATE TEMPORARY FUNCTION clean_summary AS 'hive.job1.CleanText';

CREATE TEMPORARY TABLE year2summary AS
	SELECT unix_year(Time) AS year, clean_summary(Summary) AS cleanSummary
	FROM csv;

CREATE TEMPORARY TABLE used_words AS
	SELECT year, word, count(1) AS num
	FROM year2summary
	LATERAL VIEW explode(split(trim(cleanSummary), ' +')) words AS word
	GROUP BY year, word;

CREATE TEMPORARY TABLE sorted_rows AS
	SELECT year, word, num, row_number() over (PARTITION BY year ORDER BY num DESC) AS rank
	FROM used_words;

CREATE TEMPORARY TABLE job1_result AS
	SELECT year, word, num
	FROM sorted_rows
	WHERE rank<=10 and word!='summary';

SELECT * from job1_result;