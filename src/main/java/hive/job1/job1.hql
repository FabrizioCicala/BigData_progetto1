
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
	
LOAD DATA LOCAL INPATH '/home/fabrizio/Documenti/universita/magistrale/big_data/progetto1/provaReviews.csv'
	OVERWRITE INTO TABLE csv;
	
CREATE TEMPORARY FUNCTION unix_year AS 'hive.job1.ParseDate';
CREATE TEMPORARY FUNCTION clean_summary AS 'hive.job1.CleanText';
	
CREATE TEMPORARY TABLE year2summary AS
	SELECT unix_year(Time) AS year, clean_summary(Summary) AS cleanSummary
	FROM csv;

CREATE TEMPORARY TABLE used_words AS
	SELECT year, word, count(1) AS count
	FROM year2summary
	LATERAL VIEW explode(split(cleanSummary, ' ')) words AS word
	GROUP BY year, word
	ORDER BY count DESC;

DROP TABLE job1_result;
CREATE TABLE job1_result AS
	SELECT year, COLLECT_SET(word)
	FROM used_words
	GROUP BY year;

