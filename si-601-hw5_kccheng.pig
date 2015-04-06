

R  = LOAD '/user/kevynct/yelp_academic_dataset_review.json' USING JsonLoader('votes:map[], user_id:chararray, review_id:chararray, stars:int, date:chararray, text:chararray, type:chararray, business_id:chararray');
--R  = LOAD 'yelp_kccheng.json' USING JsonLoader('votes:map[], user_id:chararray, review_id:chararray, stars:int, date:chararray, text:chararray, type:chararray, business_id:chararray');
B0 = foreach R generate flatten(TOKENIZE(LOWER(REPLACE($5, '[\\s\\.\\,\\!\\?\\-]+', ' ')))) as word, stars;
tweets_pos = FILTER B0 BY stars >= 5;
tweets_neg = FILTER B0 BY stars <= 2;

word_all = GROUP B0 by word;
word_pos = GROUP tweets_pos by word;
word_neg = GROUP tweets_neg by word;

count_all = foreach word_all generate group AS word, COUNT(B0) AS count;
count_all_s = ORDER count_all BY count DESC;
count_pos = foreach word_pos generate group AS word, COUNT(tweets_pos) AS count;
count_pos_s = ORDER count_pos BY count DESC;
count_neg = foreach word_neg generate group AS word, COUNT(tweets_neg) AS count; 
count_neg_s = ORDER count_neg BY count DESC;


STORE count_all_s INTO 'output-step-1a';
STORE count_pos_s INTO 'output-step-1b';
STORE count_neg_s INTO 'output-step-1c';
   

filter_all = FILTER count_all_s BY count > 1000;
filter_pos = JOIN filter_all BY word, count_pos_s BY word;
filter_neg = JOIN filter_all BY word, count_neg_s BY word;


STORE filter_pos INTO 'output-step-2a';
STORE filter_neg INTO 'output-step-2b';

c = GROUP B0 ALL;
summation_c = foreach c generate 'all' AS word, COUNT(B0) AS sum;

d = GROUP count_pos ALL;
summation_d = foreach d generate 'all' AS word, SUM(count_pos.count) AS sum;

e = GROUP count_neg ALL;
summation_e = foreach e generate 'all' AS word, SUM(count_neg.count) AS sum;


result_pos = foreach filter_pos generate $2 AS word, LOG((DOUBLE)$3/(DOUBLE)44841415)-LOG((DOUBLE)$1/(DOUBLE)145806031) AS score;
result_neg = foreach filter_neg generate $2 AS word, LOG((DOUBLE)$3/(DOUBLE)33566861)-LOG((DOUBLE)$1/(DOUBLE)145806031) AS score;

STORE result_pos INTO 'output-positive';
STORE result_neg INTO 'output-negative';



