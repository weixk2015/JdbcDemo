create database if not exists `jdbcdemo`;
use `jdbcdemo`;
drop table if exists student;
drop table if exists room;
CREATE table if not exists room
(
  kdno INT,
  kcno INT,
  ccno INT,
  CONSTRAINT pmk PRIMARY KEY (kdno, kcno, ccno),
  kdname VARCHAR(100) charset utf8 collate utf8_general_ci not null,
  exptime  DATETIME not null,
  papername VARCHAR(100)
);
CREATE TABLE if not exists student
(
  registno VARCHAR(11) PRIMARY KEY,
  name TEXT charset utf8 collate utf8_general_ci not null,
  kdno INT not null,
  kcno INT not null,
  ccno INT not null,
  seat INT not null,
  CONSTRAINT refer_to_room FOREIGN KEY (kdno, kcno, ccno) REFERENCES room(kdno, kcno, ccno)
);
