-module(nsm_mq_lib).
-author('Vladimir Baranov <baranoff.vladimir@gmail.com>').

%%
%% Include files
%%

-include("nsm_mq.hrl").
%%
%% Exported Functions
%%
-export([
         encode/1,
         decode/1,

         list_to_key/1, key_to_list/1
        ]).

-export([opt/3,
         override_opt/3,
         get_env/2]).

%%
%% API Functions
%%

encode(Term) ->
    term_to_binary(Term).

decode(Binary) when is_binary(Binary) ->
    binary_to_term(Binary).

%% Get option from list. Option can be either single option, like 'name', or
%% tuple {name, "Name"}. Single options are boolean, but return value can be
%% overwritten with default value. If option not found in options List - returns
%% default value.
opt(Option, List, Default) ->
    case lists:member(Option, List) of
        true ->
            true;
        false ->
            proplists:get_value(Option, List, Default)
    end.

-spec override_opt(atom(), any(), list()) -> list().

override_opt(Option, Value, Options) ->
    CleanOptions = lists:filter(
                     fun({O, _}) when O == Option -> false;
                        (O)      when O == Option -> false;
                        (_)                       -> true
                     end, Options),

    [{Option, Value} | CleanOptions].


%% @doc Get env options from application config. If parameter not set - default
%% value will be returned
-spec get_env(Par::atom(), Default::term()) -> term().

get_env(Par, DefaultValue) ->
    case application:get_env(Par) of
        undefined ->
            DefaultValue;
        {ok, Value} ->
            Value
    end.

%% @doc Decode routing key from <<"A.B.C">> to ["A", "B", "C"] to simplify
%%      matching

-spec key_to_list(binary()) -> list(string()).

key_to_list(BKey) when is_binary(BKey) ->
    LKey = binary_to_list(BKey),
    string:tokens(LKey, ".");
key_to_list(Other) ->
    throw({unexpected_key_format, Other}).

%% @doc Encode routing key from list representation to binary string.
%%      For example ["A", 'B', 1] will be encoded to <<"A.B.1">>

-spec list_to_key([term()]) -> [list()].

list_to_key(Things) when is_list(Things) ->
    WithDots = divide(Things),
    list_to_binary(lists:concat(WithDots));
list_to_key(Other) ->
    throw({unexpected_key_format, Other}).


%% Local functions

divide([])->
    [];
divide([T|[]])->
    [T];
divide([H|T]) ->
    [H, "."|divide(T)].
