-- gold.dim_client definition

-- Drop table

-- DROP TABLE gold.dim_client;

CREATE TABLE gold.dim_client (
	client_id int8 NULL,
	limit_bal numeric(18, 2) NULL,
	sex_id int2 NULL,
	education_id int2 NULL,
	marriage_id int2 NULL,
	age int4 NULL
);


-- gold.dim_education definition

-- Drop table

-- DROP TABLE gold.dim_education;

CREATE TABLE gold.dim_education (
	education_id int2 NULL,
	education_desc varchar(50) NOT NULL
);


-- gold.dim_marriage definition

-- Drop table

-- DROP TABLE gold.dim_marriage;

CREATE TABLE gold.dim_marriage (
	marriage_id int2 NULL,
	marriage_desc varchar(20) NOT NULL
);


-- gold.dim_sex definition

-- Drop table

-- DROP TABLE gold.dim_sex;

CREATE TABLE gold.dim_sex (
	sex_id int2 NULL,
	sex_desc varchar(10) NOT NULL
);


-- gold.dim_time definition

-- Drop table

-- DROP TABLE gold.dim_time;

CREATE TABLE gold.dim_time (
	time_id int8 NULL,
	"year" int4 NOT NULL,
	"month" int4 NOT NULL,
	month_name varchar(20) NULL
);


-- gold.fact_credit definition

-- Drop table

-- DROP TABLE gold.fact_credit;

CREATE TABLE gold.fact_credit (
	fact_id int8 NULL,
	client_id int8 NULL,
	time_id int8 NULL,
	pay_status int2 NULL,
	bill_amount numeric(18, 2) NULL,
	payment_amount numeric(18, 2) NULL,
	default_flag bool NULL
);