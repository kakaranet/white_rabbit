-module(nsm_mq_lib_db).
-author('Vladimir Baranov <baranoff.vladimir@gmail.com>').

%%
%% Include files
%%

-include("nsm_mq.hrl").

%% if MQ_TEST macro is defined - use this module as database backend
%-ifndef(TEST).
%-include_lib("nsm_db/include/config.hrl").
%-else.
-define(DBA, ?MODULE).
%-endif.


%%
%% Exported Functions
%%

-export([start_publisher/0,
         stop_publisher/1,

         start_worker/0,
         stop_worker/1,

         write/2,
         seq_write/2,
         dirty_write/2,

         %% private. Used in nsm_mq for creation of the queues
         seq_queue_name/1,
         seq_routing_key/1,

         write_requests_handler/2
         ]).

%-ifdef(TEST).
-export([put/1, get_data/1]).
%-endif.

%%
%% API Functions
%%

%% @doc Init publisher channel
-spec start_publisher() -> {ok, channel_pid()} | {error, any()}.

start_publisher() ->
    {ok, Channel} = nsm_mq:open([confirm]),
    {ok, Channel}.

%% @doc Close publisher channel
-spec stop_publisher(channel_pid()) -> ok.

stop_publisher(Channel) ->
    ok = nsm_mq_channel:close(Channel).


%% @doc Publish put request db exchange with collective routing key. Any worker
%% can process request.
-spec write(channel_pid(), Data::term()) -> ok.

write(Ch, Data) ->
    %% XXX: need ? receive confirmation from server
    ok = nsm_mq_channel:publish(Ch, ?DB_EX, ?DB_WORKERS_COLLECTIVE_KEY,
                               {put, Data}, [durable]).

%% @doc Publish put request to special queue with one exclusive consumer.
%% Operations will be performed in published order.
-spec seq_write(channel_pid(), term()) -> ok.

seq_write(Ch, Data) ->
    {ok, Node} = nsm_mq:node_name(),
    SeqRoutingKey = seq_routing_key(Node),
    ok = nsm_mq_channel:publish(Ch, ?DB_EX, SeqRoutingKey, {put, Data}, [durable]).

%% @doc Publish non persistent put request with collective routing key
-spec dirty_write(channel_pid(), Data::term()) -> ok.

dirty_write(Ch, Data) ->
    ok = nsm_mq_channel:publish(Ch, ?DB_EX, ?DB_WORKERS_COLLECTIVE_KEY,
                               {put, Data}, [nowait]).


%% @doc Init consumer to process db requests. For now must be one worker per
%% node, because worker will process both sequential and not sequential messages
%% Worker can be also used for 'put' requests. For only put requests, use
%% init_publisher/0.
-spec start_worker() -> {ok, channel_pid()}.

start_worker() ->
    SeqOpts = [exclusive],
    GenOpts = [{callback, {?MODULE, write_requests_handler}}],

    {ok, Node} = nsm_mq:node_name(),
    {ok, Ch} = nsm_mq:open([fair_dispatch]),

    %% start consume from sequential queue
    QueueName = seq_queue_name(Node),
    %% queue must be created when application start, so here we only consume
    {ok, _SeqCTag} = nsm_mq_channel:consume(Ch, QueueName, SeqOpts++GenOpts),

    %% start consume from collective queue
    {ok, _CollCtag} = nsm_mq_channel:consume(Ch, ?DB_WORKERS_QUEUE, GenOpts),

    {ok, Ch}.


%% @doc Stop worker. Just close channel.
-spec stop_worker(channel_pid()) -> ok.

stop_worker(Ch) ->
    ok = nsm_mq_channel:close(Ch).


-spec seq_queue_name(string()) -> string().

seq_queue_name(Node) ->
    seq_routing_key(Node).


-spec seq_routing_key(string()) -> string().

seq_routing_key(NodeName) ->
    list_to_binary(?DB_WORKERS_EXCLUSIVE_PREFIX ++ NodeName).


-spec write_requests_handler(envelope(), State::any()) -> handler_response().

write_requests_handler(#envelope{payload = {put, Data}}, _) ->
    ok = DBA=?DBA,DBA:put(Data),
    ok;

write_requests_handler(_Msg, _State) ->
    ok.

%% For testing only

%-ifdef(TEST).
-define(STORAGE_PROCESS, test_storage_process).

put(Data) when is_tuple(Data) ->
    %% if storage process is not registered - start it
    case lists:member(?STORAGE_PROCESS, registered()) of
        false ->
            %% storage process loop
            F = fun(Storage, Fun)->
                        receive
                            {put, Key, Val} ->
                                Fun(dict:store(Key, Val, Storage), Fun);
                            {get, From, Key}  ->
                                From!{Key, dict:find(Key, Storage)},
                                Fun(Storage, Fun)
                        end
                end,
            Pid = spawn_link(fun()-> F(dict:new(), F) end),
            true = register(?STORAGE_PROCESS, Pid);
        _ ->
            ok
    end,

    %%XXX: dirty hack for testing data store. Put data into registered process
    %% dict, key position = 2, to allow records usage.
    Key = element(2, Data),

    ?STORAGE_PROCESS!{put, Key, Data},

    ok.

get_data(Key) ->
    ?STORAGE_PROCESS!{get, self(), Key},
    receive
        {Key, Value} ->
            Value
    after 300 ->
            throw({timeout, get_data_from_storage_process})
    end.


%-endif.

%%
%% Local Functions
%%


