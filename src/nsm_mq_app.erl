-module(nsm_mq_app).

-behaviour(application).

%% Application callbacks
-export([start/0, start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start() ->
    application:start(nsm_mq).

start(_StartType, _StartArgs) ->
    nsm_mq_sup:start_link().

stop(_State) ->
    ok.
