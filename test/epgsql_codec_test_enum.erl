-module(epgsql_codec_test_enum).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3]).


init(Choices, _) -> Choices.

names() ->
    [my_type].

encode(Atom, my_type, Choices) ->
    true = lists:member(Atom, Choices),
    atom_to_binary(Atom, utf8).

decode(Bin, my_type, Choices) ->
    Atom = binary_to_existing_atom(Bin, utf8),
    true = lists:member(Atom, Choices),
    Atom.
