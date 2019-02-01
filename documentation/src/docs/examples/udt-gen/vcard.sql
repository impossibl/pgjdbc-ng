CREATE TYPE title as enum ('mr', 'mrs', 'ms', 'dr');
CREATE TYPE address as (street text, city text, state char(2), zip char(5));
CREATE TYPE v_card as (id int4, name text, title title, addresses address[]);
