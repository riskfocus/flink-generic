CREATE DATABASE if not exists test;
USE test;
CREATE TABLE if not exists rate (
    id INT PRIMARY KEY,
    maturity VARCHAR(255) NOT NULL,
    rate DECIMAL(19,4)
) ENGINE=INNODB;
