-- script to create test schema for epgsql unit tests --
--
-- this script should be run as the same user the tests will be run as,
-- so that the test for connecting as the 'current user' succeeds
--
-- the following lines must be added to pg_hba.conf for all tests to
-- succeed:
--
-- host    epgsql_test_db1 epgsql_test             127.0.0.1/32    trust
-- host    epgsql_test_db1 epgsql_test_md5         127.0.0.1/32    md5
-- host    epgsql_test_db1 epgsql_test_cleartext   127.0.0.1/32    password
-- hostssl epgsql_test_db1 epgsql_test_cert        127.0.0.1/32    cert
--
-- any 'trust all' must be commented out for the invalid password test
-- to succeed.
--
-- ssl support must be configured, and the sslinfo contrib module
-- loaded for the ssl tests to succeed.


CREATE USER epgsql_test;
CREATE USER epgsql_test_md5 WITH PASSWORD 'epgsql_test_md5';
CREATE USER epgsql_test_cleartext WITH PASSWORD 'epgsql_test_cleartext';
CREATE USER epgsql_test_cert;

CREATE DATABASE epgsql_test_db1 WITH ENCODING 'UTF8';
CREATE DATABASE epgsql_test_db2 WITH ENCODING 'UTF8';

GRANT ALL ON DATABASE epgsql_test_db1 to epgsql_test;
GRANT ALL ON DATABASE epgsql_test_db1 to epgsql_test_md5;
GRANT ALL ON DATABASE epgsql_test_db1 to epgsql_test_cleartext;
GRANT ALL ON DATABASE epgsql_test_db2 to epgsql_test;

\c epgsql_test_db1;

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
  c_date date,
  c_time time,
  c_timetz timetz,
  c_timestamp timestamp,
  c_timestamptz timestamptz,
  c_interval interval);

CREATE LANGUAGE plpgsql;

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