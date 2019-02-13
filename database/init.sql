CREATE TABLE users (
    username VARCHAR(20) NOT NULL PRIMARY KEY,
    balance FLOAT NOT NULL
);

CREATE TABLE stocks (
	username VARCHAR(20) references users(username),
	stock_symbol VARCHAR(3) NOT NULL,
	stock_quantity INT NOT NULL,
	PRIMARY KEY (username, stock_symbol)
);

CREATE TABLE reserved (
	reservationid SERIAL PRIMARY KEY,
	type VARCHAR(5) NOT NULL,
	username VARCHAR(20) references users(username),
	stock_symbol VARCHAR(3) NOT NULL,
	stock_quantity INT NOT NULL,
	price FLOAT NOT NULL,
	amount FLOAT NOT NULL,
	timestamp FLOAT NOT NULL
);

CREATE TABLE triggers (
	username VARCHAR(20) NOT NULL references users(username)
	ON DELETE CASCADE ON UPDATE CASCADE,
	stock_symbol VARCHAR(3) NOT NULL,
	type VARCHAR(5) NOT NULL,
	trigger_amount FLOAT,
	transaction_amount FLOAT NOT NULL,
	PRIMARY KEY (username, stock_symbol, type)
);

