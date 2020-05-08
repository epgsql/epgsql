%%% coding: utf-8
%%% @doc
%%% This is a helper module that will validate a utf-8
%%% string based on sasl_prep profile as defined in
%%% https://tools.ietf.org/html/rfc4013
%%% @end

-module(epgsql_sasl_prep_profile).

-export([ validate/1
        ]).

-spec validate(iolist()) -> iolist().
validate(Str) ->
    CharL = unicode:characters_to_list(Str, utf8),
    lists:foreach(fun(F) ->
                      lists:any(F, CharL)
                          andalso error({non_valid_scram_password, Str})
                  end, [ fun is_non_asci_space_character/1
                       , fun is_ascii_control_character/1
                       , fun is_non_ascii_control_character/1
                       , fun is_private_use_characters/1
                       , fun is_non_character_code_points/1
                       , fun is_surrogate_code_points/1
                       , fun is_inappropriate_for_plain_text_char/1
                       , fun is_inappropriate_for_canonical_representation_char/1
                       , fun is_change_display_properties_or_deprecated_char/1
                       , fun is_tagging_char/1 ]),
    Str.

%% @doc Return true if the given character is a non-ASCII space character
%% as defined by https://tools.ietf.org/html/rfc3454#appendix-C.1.2
-spec is_non_asci_space_character(char()) -> boolean().
is_non_asci_space_character(C) ->
    C == 16#00A0
        orelse C == 16#1680
        orelse (16#2000 =< C andalso C =< 16#200B)
        orelse C == 16#202F
        orelse C == 16#205F
        orelse C == 16#3000.

%% @doc Return true if the given character is an ASCII control character
%% as defined by https://tools.ietf.org/html/rfc3454#appendix-C.2.1
-spec is_ascii_control_character(char()) -> boolean().
is_ascii_control_character(C) ->
    C =< 16#001F orelse C == 16#007F.

%% @doc Return true if the given character is a non-ASCII control character
%% as defined by https://tools.ietf.org/html/rfc3454#appendix-C.2.2
-spec is_non_ascii_control_character(char()) -> boolean().
is_non_ascii_control_character(C) ->
    (16#0080 =< C andalso C =< 16#009F)
        orelse C == 16#06DD
        orelse C == 16#070F
        orelse C == 16#180E
        orelse C == 16#200C
        orelse C == 16#200D
        orelse C == 16#2028
        orelse C == 16#2029
        orelse C == 16#2060
        orelse C == 16#2061
        orelse C == 16#2062
        orelse C == 16#2063
        orelse (16#206A =< C andalso C =< 16#206F)
        orelse C == 16#FEFF
        orelse (16#FFF9 =< C andalso C =< 16#FFFC)
        orelse (16#1D173 =< C andalso C =< 16#1D17A).

%% @doc Return true if the given character is a private use character
%% as defined by https://tools.ietf.org/html/rfc3454#appendix-C.3
-spec is_private_use_characters(char()) -> boolean().
is_private_use_characters(C) ->
    (16#E000 =< C andalso C =< 16#F8FF)
         orelse (16#F000 =< C andalso C =< 16#FFFFD)
        orelse (16#100000 =< C andalso C =< 16#10FFFD).

%% @doc Return true if the given character is a non-character code point
%% as defined by https://tools.ietf.org/html/rfc3454#appendix-C.4
-spec is_non_character_code_points(char()) -> boolean().
is_non_character_code_points(C) ->
    (16#FDD0 =< C andalso C =< 16#FDEF)
        orelse (16#FFFE =< C andalso C =< 16#FFFF)
        orelse (16#1FFFE =< C andalso C =< 16#1FFFF)
        orelse (16#2FFFE =< C andalso C =< 16#2FFFF)
        orelse (16#3FFFE =< C andalso C =< 16#3FFFF)
        orelse (16#4FFFE =< C andalso C =< 16#4FFFF)
        orelse (16#5FFFE =< C andalso C =< 16#5FFFF)
        orelse (16#6FFFE =< C andalso C =< 16#6FFFF)
        orelse (16#7FFFE =< C andalso C =< 16#7FFFF)
        orelse (16#8FFFE =< C andalso C =< 16#8FFFF)
        orelse (16#9FFFE =< C andalso C =< 16#9FFFF)
        orelse (16#AFFFE =< C andalso C =< 16#AFFFF)
        orelse (16#BFFFE =< C andalso C =< 16#BFFFF)
        orelse (16#CFFFE =< C andalso C =< 16#CFFFF)
        orelse (16#DFFFE =< C andalso C =< 16#DFFFF)
        orelse (16#EFFFE =< C andalso C =< 16#EFFFF)
        orelse (16#FFFFE =< C andalso C =< 16#FFFFF)
        orelse (16#10FFFE =< C andalso C =< 16#10FFFF).

%% @doc Return true if the given character is a surrogate code point as defined by
%% https://tools.ietf.org/html/rfc3454#appendix-C.5
-spec is_surrogate_code_points(char()) -> boolean().
is_surrogate_code_points(C) ->
    16#D800 =< C andalso C =< 16#DFFF.

%% @doc Return true if the given character is inappropriate for plain text characters
%% as defined by https://tools.ietf.org/html/rfc3454#appendix-C.6
-spec is_inappropriate_for_plain_text_char(char()) -> boolean().
is_inappropriate_for_plain_text_char(C) ->
    C == 16#FFF9
        orelse C == 16#FFFA
        orelse C == 16#FFFB
        orelse C == 16#FFFC
        orelse C == 16#FFFD.

%% @doc Return true if the given character is inappropriate for canonical representation
%% as defined by https://tools.ietf.org/html/rfc3454#appendix-C.7
-spec is_inappropriate_for_canonical_representation_char(char()) -> boolean().
is_inappropriate_for_canonical_representation_char(C) ->
    16#2FF0 =< C andalso C =< 16#2FFB.

%% @doc Return true if the given character is change display properties or deprecated
%% characters as defined by https://tools.ietf.org/html/rfc3454#appendix-C.8
-spec is_change_display_properties_or_deprecated_char(char()) -> boolean().
is_change_display_properties_or_deprecated_char(C) ->
    C == 16#0340
        orelse C == 16#0341
        orelse C == 16#200E
        orelse C == 16#200F
        orelse C == 16#202A
        orelse C == 16#202B
        orelse C == 16#202C
        orelse C == 16#202D
        orelse C == 16#202E
        orelse C == 16#206A
        orelse C == 16#206B
        orelse C == 16#206C
        orelse C == 16#206D
        orelse C == 16#206E
        orelse C == 16#206F.

%% @doc Return true if the given character is a tagging character as defined by
%% https://tools.ietf.org/html/rfc3454#appendix-C.9
-spec is_tagging_char(char()) -> boolean().
is_tagging_char(C) ->
    C == 16#E0001 orelse
        (16#E0020 =< C andalso C =< 16#E007F).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

normalize_test() ->
    ?assertEqual(<<"123 !~">>, validate(<<"123 !~">>)),
    ?assertEqual(<<"привет"/utf8>>, validate(<<"привет"/utf8>>)),
    ?assertEqual(<<"Χαίρετε"/utf8>>, validate(<<"Χαίρετε"/utf8>>)),
    ?assertEqual(<<"你好"/utf8>>, validate(<<"你好"/utf8>>)),
    ?assertError({non_valid_scram_password, _},
                 validate(<<"boom in the last char  ́"/utf8>>)).

-endif.