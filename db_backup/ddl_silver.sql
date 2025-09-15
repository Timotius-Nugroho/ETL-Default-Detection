-- silver.bill_statements definition

-- Drop table

-- DROP TABLE silver.bill_statements;

CREATE TABLE silver.bill_statements (
	id int8 NULL,
	client_id int8 NULL,
	"month" date NULL,
	bill_amount numeric(18, 2) NULL
);


-- silver.clients definition

-- Drop table

-- DROP TABLE silver.clients;

CREATE TABLE silver.clients (
	client_id int8 NULL,
	limit_bal numeric(18, 2) NULL,
	sex int2 NULL,
	education int2 NULL,
	marriage int2 NULL,
	age int2 NULL
);


-- silver.payment_history definition

-- Drop table

-- DROP TABLE silver.payment_history;

CREATE TABLE silver.payment_history (
	id int8 NULL,
	client_id int8 NULL,
	"month" date NULL,
	pay_status int2 NULL
);


-- silver.payments definition

-- Drop table

-- DROP TABLE silver.payments;

CREATE TABLE silver.payments (
	id int8 NULL,
	client_id int8 NULL,
	"month" date NULL,
	payment_amount numeric(18, 2) NULL
);