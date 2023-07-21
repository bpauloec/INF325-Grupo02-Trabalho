CREATE KEYSPACE reclamacoes WITH REPLICATION = { 'class' : 'SimpleStrategy' , 'replication_factor' : 3 };

USE  reclamacoes;

CREATE TABLE complaint (
ticket_id uuid,
user_id uuid,
order_id uuid,
product_id uuid,
seller_id uuid,
creation_date timestamp,
title text,
type text,
description text,
status text,
replies map<timestamp, text>,
PRIMARY KEY (ticket_id)
);

CREATE TABLE user (
id uuid,
cpf text,
name varchar,
address text,
country varchar,
email varchar,
PRIMARY KEY (id)
);

CREATE TABLE seller (
id uuid,
cnpj text,
name varchar,
address text,
country varchar,
email varchar,
PRIMARY KEY (id)
);

CREATE TABLE product (
id uuid,
name text,
description text,
price decimal,
images list<blob>,
sellers list<uuid>,
PRIMARY KEY (id)
);

CREATE TABLE order_info (
id uuid,
buyer_id uuid,
products_quantity map<uuid, int>,
sellers list<uuid>,
delivery_address text,
total_cost decimal,
PRIMARY KEY (id)
);