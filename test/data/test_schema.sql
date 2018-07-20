
-- script to create test schema for epgsql unit tests --
--
-- this script should be run as the same user the tests will be run as,
-- so that the test for connecting as the 'current user' succeeds
--
-- ssl support must be configured, and the sslinfo contrib module
-- loaded for the ssl tests to succeed.

CREATE USER epgsql_test;
CREATE USER epgsql_test_md5 WITH PASSWORD 'epgsql_test_md5';
CREATE USER epgsql_test_cleartext WITH PASSWORD 'epgsql_test_cleartext';
CREATE USER epgsql_test_cert;
CREATE USER epgsql_test_replication WITH REPLICATION PASSWORD 'epgsql_test_replication';
SET password_encryption TO 'scram-sha-256';
CREATE USER epgsql_test_scram WITH PASSWORD 'epgsql_test_scram';
SET password_encryption TO 'md5';

CREATE DATABASE epgsql_test_db1 WITH ENCODING 'UTF8';
CREATE DATABASE epgsql_test_db2 WITH ENCODING 'UTF8';

GRANT ALL ON DATABASE epgsql_test_db1 to epgsql_test;
GRANT ALL ON DATABASE epgsql_test_db1 to epgsql_test_md5;
GRANT ALL ON DATABASE epgsql_test_db1 to epgsql_test_scram;
GRANT ALL ON DATABASE epgsql_test_db1 to epgsql_test_cleartext;
GRANT ALL ON DATABASE epgsql_test_db2 to epgsql_test;

\c epgsql_test_db1;

CREATE TABLE schema_version (version varchar);

-- This requires Postgres to be compiled with SSL:
-- http://www.postgresql.org/docs/9.4/static/sslinfo.html
CREATE EXTENSION sslinfo;

CREATE EXTENSION hstore;
CREATE EXTENSION postgis;

CREATE TABLE test_table1 (id integer primary key, value text);

INSERT INTO test_table1 (id, value) VALUES (1, 'one');
INSERT INTO test_table1 (id, value) VALUES (2, 'two');

CREATE TABLE test_table2 (
  c_bool bool,
  c_char char,
  c_int2 int2,
  c_int4 int4,
  c_int8 int8,
  c_float4 float4,
  c_float8 float8,
  c_bytea bytea,
  c_text text,
  c_varchar varchar(64),
  c_uuid uuid,
  c_date date,
  c_time time,
  c_timetz timetz,
  c_timestamp timestamp,
  c_timestamptz timestamptz,
  c_interval interval,
  c_hstore hstore,
  c_point point,
  c_geometry geometry,
  c_cidr cidr,
  c_inet inet,
  c_macaddr macaddr,
  c_int4range int4range,
  c_int8range int8range,
  c_json json,
  c_jsonb jsonb,
  c_tsrange tsrange,
  c_tstzrange tstzrange,
  c_daterange daterange
  );

-- CREATE LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_test1(_id integer, _value text)
returns integer
as $$
begin
  insert into test_table1 (id, value) values (_id, _value);
  return _id;
end
$$ language plpgsql;

CREATE OR REPLACE FUNCTION do_nothing()
returns void
as $$
begin
end
$$ language plpgsql;

GRANT ALL ON TABLE test_table1 TO epgsql_test;
GRANT ALL ON TABLE test_table2 TO epgsql_test;
GRANT ALL ON FUNCTION insert_test1(integer, text) TO epgsql_test;
GRANT ALL ON FUNCTION do_nothing() TO epgsql_test;
