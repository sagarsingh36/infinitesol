-- Databricks notebook source
create database if not exists f1_raw

-- COMMAND ----------

create database if not exists f1_processed

-- COMMAND ----------

create database if not exists f1_presentation

-- COMMAND ----------

show databases

-- COMMAND ----------


drop table if exists f1_raw.circutes;
create table if not exists f1_raw.circutes( circuteId Int, circuteRef STRING, name STRING, location STRING, country STRING, lat DOUBLE, lng DOUBLE, alt INT, url STRING )
using csv
options (path "abfss://raw@fractalstorage9661.dfs.core.windows.net/circuits.csv", header True);

-- COMMAND ----------

select * from f1_raw.circutes

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races( raceId Int, year Int, round Int, circuteid int, name STRING, date date, time string, url STRING )
using csv
options (path "abfss://raw@fractalstorage9661.dfs.core.windows.net/races.csv", header True);

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING)
using json
options (path "abfss://raw@fractalstorage9661.dfs.core.windows.net/constructors.json", header True);

-- COMMAND ----------

select * from f1_raw.constructors

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(driverId INT, driverRef STRING, number Int, code STRING, name STRUCT<forename: STRING, surname: STRING>, dob date, nationality string, url string)
using json
options (path "abfss://raw@fractalstorage9661.dfs.core.windows.net/drivers.json", header True);

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING json
OPTIONS (path "abfss://raw@fractalstorage9661.dfs.core.windows.net/qualifying", multiLine true);

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "abfss://raw@fractalstorage9661.dfs.core.windows.net/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
using json
options (path "abfss://raw@fractalstorage9661.dfs.core.windows.net/pit_stops.json", multiLine True);

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(resultId INT, raceId int, driverId Int, constructorId int, number int, grid Int, position Int, positionText string, positionOrder Int,points Int, laps Int, time string, milliseconds Int, fastestLap Int, rank Int, fastestLapTime string, fastestLapSpeed float, statusId string)
using json
options (path "abfss://raw@fractalstorage9661.dfs.core.windows.net/results.json", header True);

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

create  database if not exists f1_processed

-- COMMAND ----------

constructors.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circutes")

-- COMMAND ----------

select * from f1_processed.constructors
