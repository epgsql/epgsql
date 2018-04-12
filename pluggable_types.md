# Pluggable types

It's possible to make a custom datatype encoder/decoder as well as to change encoding/decoding
of existing supported datatype.
You can't have specific decoding rules for specific column or for specific query. Codec update
affects any occurence of this datatype for this connection.

## Possible usecases

* Decode JSON inside epgsql
* Change datetime representation
* Add support for standard datatype that isn't supported by epgsql yet
* Add support for contrib datatypes
* Add codecs for your own custom datatypes (eg
  [implemented on C level](https://www.postgresql.org/docs/current/static/xtypes.html) or
  created by [CREATE TYPE](https://www.postgresql.org/docs/current/static/sql-createtype.html))

## This can be done by following steps

### Implement epgsql_codec behaviour callback module

See [epgsql_codec](src/epgsql_codec.erl)

This module should have following functions exported:

```erlang
init(any(), epgsql_sock:pg_sock()) -> codec_state().
```
Will be called only once on connection setup or when `update_type_cache/2` is called.
Should initialize codec's internal state (if needed). This state will be passed as 1st
argument to other callbacks later.

```erlang
names() -> [epgsql:type_name()].
```
Will be called immediately after init. It should return list of postgresql type names
this codec is able to handle. Names should be the same as in column `typname` of `pg_type`
table.

```erlang
encode(Data :: any(), epgsql:type_name(), codec_state()) -> iodata().
```
Will be called when parameter of matching type is passed to `equery` or `bind` etc.
2nd argument is the name of matching type (useful when `names/0` returns more than one name).
It should convert data to iolist / binary in a postgresql binary format representation.
Postgresql binary format usualy not documented, so you most likely end up checking postgresql
[source code](https://github.com/postgres/postgres/tree/master/src/backend/utils/adt).
*TIP*: it's usualy more efficient to encode data as iolist, because in that case it will be
written directly to socket without any extra copying. So, you don't need to call
`iolist_to_binary/1` on your data before returning it from this function.

```erlang
decode(Data :: binary(), epgsql:type_name(), codec_state()) -> any()
```
If `equery` or `execute` returns a dataset that has columns of matching type, this function
will be called for each "cell" of this type. It should parse postgresql binary format and
return appropriate erlang representation.

```erlang
decode_text(Data :: binary(), epgsql:type_name(), codec_state()) -> any().
```
Optional callback. Will be called (if defined) in the same situation as `decode/3`, but for
`squery` command results and data will be in postgresql text, not binary representation.
By default epgsql will just return it as is.

It would be nice to also define and export `in_data()`, `out_data()` and `data()` typespecs.

Example: if your codec's `names/0` returns `[my_type, my_other_type]` and following command was
executed:

```erlang
epgsql:equery(C, "SELECT $1::my_type, $1::my_type", [my_value])
```

Then `encode(my_value, my_type, codec_state())` will be called (only once). And, since we are doing select
of a 2 values of type `my_type`, callback `decode(binary(), my_type, codec_state())` will be
called 2 times.

### Load this codec into epgsql

It can be done by calling `epgsql:update_type_cache(Connection, [{CallbackModuleName, InitOptions}])` or
by providing `{codecs, [{CallbackModuleName, InitOptions}]}` connect option.

You may define new datatypes as well as redefine already supported ones.

## Tips

* When you just want to slightly change default decoding/encoding, it may be easier to emulate
  inheritance by calling default codec's functions and then modifying what they return
* Again, try to return iolists from `encode/3` when ever possible
* You may pass options to `init/2`. It's the 2nd element of the tuple `{ModName, Opts}`.
* You may use some context information from connection (it's internal record
  passed as 2nd argument to `init/2`). See [epgsql_sock.erl](src/epgsql_sock.erl) for API functions.
* Note that any error in callback functions will cause crash of epgsql connection process!
