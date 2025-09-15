--external

-- "external".clients definition

-- Drop table

-- DROP TABLE "external".clients;

CREATE TABLE "external".clients (
	client_id int8 NOT NULL,
	limit_bal numeric(18, 2) NULL,
	sex int2 NULL,
	education int2 NULL,
	marriage int2 NULL,
	age int2 NULL,
	CONSTRAINT clients_pkey PRIMARY KEY (client_id)
);


-- "external".bill_statements definition

-- Drop table

-- DROP TABLE "external".bill_statements;

CREATE TABLE "external".bill_statements (
	id int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL,
	client_id int8 NULL,
	"month" date NULL,
	bill_amount numeric(18, 2) NULL,
	CONSTRAINT bill_statements_pkey PRIMARY KEY (id),
	CONSTRAINT bill_statements_client_id_fkey FOREIGN KEY (client_id) REFERENCES "external".clients(client_id)
);


-- "external".payment_history definition

-- Drop table

-- DROP TABLE "external".payment_history;

CREATE TABLE "external".payment_history (
	id int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL,
	client_id int8 NULL,
	"month" date NULL,
	pay_status int2 NULL,
	CONSTRAINT payment_history_pkey PRIMARY KEY (id),
	CONSTRAINT payment_history_client_id_fkey FOREIGN KEY (client_id) REFERENCES "external".clients(client_id)
);


-- "external".payments definition

-- Drop table

-- DROP TABLE "external".payments;

CREATE TABLE "external".payments (
	id int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL,
	client_id int8 NULL,
	"month" date NULL,
	payment_amount numeric(18, 2) NULL,
	CONSTRAINT payments_pkey PRIMARY KEY (id),
	CONSTRAINT payments_client_id_fkey FOREIGN KEY (client_id) REFERENCES "external".clients(client_id)
);