DROP TABLE IF EXISTS usercommands;
DROP TABLE IF EXISTS quoteservers;
DROP TABLE IF EXISTS accounttransactions;
DROP TABLE IF EXISTS systemevents;
DROP TABLE IF EXISTS errorevents;
DROP TABLE IF EXISTS debugevents;


CREATE TABLE usercommands (
    timestamp BIGINT NOT NULL,
	server VARCHAR(20) NOT NULL,
	transaction_num INTEGER NOT NULL,
	command VARCHAR(20) NOT NULL,
	username VARCHAR(20),
	stock_symbol VARCHAR(3),
	funds FLOAT
);

CREATE TABLE quoteservers (
    timestamp BIGINT NOT NULL,
	server VARCHAR(20) NOT NULL,
	transaction_num INTEGER NOT NULL,
	price FLOAT NOT NULL,
	stock_symbol VARCHAR(3) NOT NULL,
	username VARCHAR(20) NOT NULL,
	quote_server_time BIGINT NOT NULL,
	crypto_key VARCHAR(80) NOT NULL
);

CREATE TABLE accounttransactions (
    timestamp BIGINT NOT NULL,
	server VARCHAR(20) NOT NULL,
	transaction_num INTEGER NOT NULL,
	action VARCHAR(6) NOT NULL,
	username VARCHAR(20) NOT NULL,
	funds FLOAT
);

CREATE TABLE systemevents (
    timestamp BIGINT NOT NULL,
	server VARCHAR(20) NOT NULL,
	transaction_num INTEGER NOT NULL,
	command VARCHAR(20) NOT NULL,
	username VARCHAR(20),
	stock_symbol VARCHAR(3),
	funds FLOAT
);

CREATE TABLE errorevents (
    timestamp BIGINT NOT NULL,
	server VARCHAR(20) NOT NULL,
	transaction_num INTEGER NOT NULL,
	command VARCHAR(20) NOT NULL,
	username VARCHAR(20),
	stock_symbol VARCHAR(3),
	funds FLOAT,
	error_message VARCHAR(100)
);

CREATE TABLE debugevents (
    timestamp BIGINT NOT NULL,
	server VARCHAR(20) NOT NULL,
	transaction_num INTEGER NOT NULL,
	command VARCHAR(20) NOT NULL,
	username VARCHAR(20),
	stock_symbol VARCHAR(3),
	funds FLOAT,
	debug_message VARCHAR(100)
);
