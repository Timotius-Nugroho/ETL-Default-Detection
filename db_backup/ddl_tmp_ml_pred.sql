-- tmp_ml_pred.credit_card_default definition

-- Drop table

-- DROP TABLE tmp_ml_pred.credit_card_default;

CREATE TABLE tmp_ml_pred.credit_card_default (
	id int8 NULL,
	limit_bal numeric(18, 2) NULL,
	sex text NULL,
	education text NULL,
	marriage text NULL,
	age int4 NULL,
	pay_0 int4 NULL,
	pay_2 int4 NULL,
	pay_3 int4 NULL,
	pay_4 int4 NULL,
	pay_5 int4 NULL,
	pay_6 int4 NULL,
	bill_amt1 numeric(18, 2) NULL,
	bill_amt2 numeric(18, 2) NULL,
	bill_amt3 numeric(18, 2) NULL,
	bill_amt4 numeric(18, 2) NULL,
	bill_amt5 numeric(18, 2) NULL,
	bill_amt6 numeric(18, 2) NULL,
	pay_amt1 numeric(18, 2) NULL,
	pay_amt2 numeric(18, 2) NULL,
	pay_amt3 numeric(18, 2) NULL,
	pay_amt4 numeric(18, 2) NULL,
	pay_amt5 numeric(18, 2) NULL,
	pay_amt6 numeric(18, 2) NULL,
	default_payment_next_month bool DEFAULT false NULL
);