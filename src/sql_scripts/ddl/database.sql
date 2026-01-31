/*
 * Daily Banking Operations ETL Pipeline
 * An automated process for daily processing of banking transaction data that:
 * 	Receives a daily dump of financial transactions
 * 	Cleanses, validates, and loads the data into a secure data warehouse
 * 	Generates regulatory and operational daily reports
 * 	Test datasets containing 3 days of anonymized transaction data are provided for 
 * 	development and validation.
 */

SELECT
	OID,
	DATNAME
from pg_database;
-- OID (uint32) - Object Identifier

CREATE DATABASE mipt_project;

CREATE SCHEMA bank;

--accounts
--DROP TABLE bank.accouns_scd2;
CREATE TABLE bank.accounts_scd2 (
	account 		VARCHAR(128) NULL,
	valid_to 		DATE NULL,
	client 			VARCHAR(128) NULL,
	create_dt 		DATE NULL,
	update_dt 		DATE NULL,
	effective_from	DATE DEFAULT now()::DATE,
	effective_to 	DATE,
	deleted_flag 	BOOLEAN DEFAULT FALSE,
	is_current		BOOLEAN DEFAULT TRUE
);

INSERT INTO bank.accounts_scd2 (account, valid_to, client, create_dt, update_dt)
SELECT
	account,
	valid_to,
	client,
	create_dt,
	update_dt
FROM bank.accounts;

--cards
CREATE TABLE bank.cards_scd2 (
	card_num 		VARCHAR(128) NULL,
	account 		VARCHAR(128) NULL,
	create_dt 		DATE NULL,
	update_dt 		DATE NULL,
	effective_from	DATE DEFAULT now()::DATE,
	effective_to 	DATE,
	deleted_flag 	BOOLEAN DEFAULT FALSE,
	is_current		BOOLEAN DEFAULT TRUE
);

INSERT INTO bank.cards_scd2 (card_num, account, create_dt, update_dt)
SELECT
	card_num,
	account,
	create_dt,
	update_dt
FROM bank.cards;

--clients
CREATE TABLE bank.clients_scd2 (
	client_id 		VARCHAR(128) NULL,
	last_name 		VARCHAR(128) NULL,
	first_name 		VARCHAR(128) NULL,
	patronymic 		VARCHAR(128) NULL,
	date_of_birth 	DATE NULL,
	passport_num 	VARCHAR(128) NULL,
	passport_valid_to DATE NULL,
	phone			VARCHAR(128) NULL,
	create_dt 		DATE NULL,
	update_dt 		DATE NULL,
	effective_from	DATE DEFAULT now()::DATE,
	effective_to 	DATE,
	deleted_flag 	BOOLEAN DEFAULT FALSE,
	is_current		BOOLEAN DEFAULT TRUE
);

INSERT INTO bank.clients_scd2 (
	client_id,
	last_name,
	first_name,
	patronymic,
	date_of_birth,
	passport_num,
	passport_valid_to,
	phone,
	create_dt,
	update_dt
)
SELECT 
	client_id,
	last_name,
	first_name,
	patronymic,
	date_of_birth,
	passport_num,
	passport_valid_to,
	phone,
	create_dt,
	update_dt
FROM bank.clients;

--passport_blacklist
CREATE TABLE bank.passport_blacklist_scd2 (
	passport_num 	VARCHAR(128) NULL,
	entry_dt 		DATE NULL,
	effective_from	DATE DEFAULT now()::DATE,
	effective_to 	DATE,
	deleted_flag 	BOOLEAN DEFAULT FALSE,
	is_current		BOOLEAN DEFAULT TRUE
);

--terminals
CREATE TABLE bank.terminals_scd2 (
	terminal_id		VARCHAR(128),
	terminal_type	VARCHAR(128),
	terminal_city	VARCHAR(128),
	terminal_address	VARCHAR(128),
	effective_from	DATE DEFAULT now()::DATE,
	effective_to 	DATE,
	deleted_flag 	BOOLEAN DEFAULT FALSE,
	is_current		BOOLEAN DEFAULT TRUE
);

--transactions
CREATE TABLE bank.transactions_scd2 (
	trans_id		VARCHAR(128),
	trans_date		DATE,
	card_num 		VARCHAR(128) NULL,
	oper_type 		VARCHAR(128) NULL,
	amt				NUMERIC(10, 2),
	effective_from	DATE DEFAULT now()::DATE,
	effective_to 	DATE,
	deleted_flag 	BOOLEAN DEFAULT FALSE,
	is_current		BOOLEAN DEFAULT TRUE	
);

