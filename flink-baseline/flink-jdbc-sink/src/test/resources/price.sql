CREATE TABLE price (
    id INT PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    sourceIp VARCHAR(255) NOT NULL,
    price DECIMAL(19,4)
)  ENGINE=INNODB;
