--DDL--

CREATE TABLE  t_movie_zsp(
MovieID STRING, MovieName STRING, MovieType STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim" = "::")
LOCATION '/data/hive/movies.dat';

----
create  table t_user_zsp(userid int, sex string, age int, occupation int, zipcode int
) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim" = "::") location '/data/hive/users.dat’;
---
create  table t_rating_zsp(userid int, movieid int, rate smallint, times bigint) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim" = "::") LOCATION '/data/hive/ratings.dat';

--DML--
Select u.age,avg(r.rate) from t_rating_zsp r inner join t_user_zsp  u on r.userid = u.userid where r.movieid = 2116 GROUP BY  u.age;

Select u.sex,m.moviename, avg(r.rate) as a, count(*) as n from t_user_zsp u join t_rating_zsp r  on (r.userid = u.userid) join t_movie_zsp m on (r.movieid = m.movieid) where u.sex='M' group by m.moviename,u.sex having n > 50  order by a desc limit 10;