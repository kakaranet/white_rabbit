-module(nsm_mq_channel_sup).
-author('Vladimir Baranov <baranoff.vladimir@gmail.com>').

-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([start_channel/2]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_channel(Conn, Options) ->
    supervisor:start_child(?MODULE, [Conn, Options]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{nsm_mq_channel, {nsm_mq_channel, start_link, []},
            temporary, brutal_kill, worker, [nsm_mq_channel]}]}}.

