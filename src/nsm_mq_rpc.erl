-module(nsm_mq_rpc).
-author('Vladimir Baranov <baranoff.vladimir@gmail.com>').

%%
%% Include files
%%
-include("nsm_mq.hrl").

%%
%% Exported Functions
%%
-export([call/2,
         call/3,

         join/2,
         join/3,

         serve/1,
         serve/2,
         serve/3
         ]).

-define(DEFAULT_TIMEOUT, 5000).

-type rpc_callback() :: function()|{Module::atom(), Func::atom()}.

%%
%% API Functions
%%

%% @doc Send synchronous request to specified rpc queue with default timeout.
-spec call(routing_key(), term()) -> {ok, Result::term()}|{error, Reason::term()}.

call(RoutingKey, Request) when is_binary(RoutingKey) ->
    nsm_mq_rpc_srv:call(RoutingKey, Request, ?DEFAULT_TIMEOUT).

%% @doc Send synchronous request to specified rpc queue with timeout.
%% After timeout call will return with error
-spec call(channel_pid(), routing_key(), integer()|infinity) ->
          {ok, Result::term()}|{error, Reason::term()}.

call(RoutingKey, Request, Timeout) when is_binary(RoutingKey) ->
    nsm_mq_rpc_srv:call(RoutingKey, Request, Timeout).


%% @doc Join as worker to rpc queue. Callback process requests.
%% For example: Callback={Module, Func} then Module:Func(Request) invoking
%% when request arrived.
-spec join(binary(), rpc_callback()) -> ok.

join(RPCQueue, Callback) ->
    %% create channel with fair dispatching
    {ok, Channel} = nsm_mq:open([fair_dispatch]),
    join(Channel, RPCQueue, Callback).


-spec join(channel_pid(), binary(), rpc_callback())-> ok.

join(Channel, RPCQueue, Callback) ->
    %% try to create RPC queue and start consume requests.
    %% If queue is not exists it will be created.
    {ok, RPCQueue} = nsm_mq_channel:create_queue(Channel, RPCQueue,
                                                 [{durable, true},
                                                  {auto_delete, false}]),
    Options = [{exclusive, false},
               {callback, server_callback(Callback)},
               {state, Channel}],
    {ok, _CTag} = nsm_mq_channel:consume(Channel, RPCQueue, Options),
    ok.

%% @doc Join to rpc queue as exclusive consumer. In serve/2 queue name
%% is <<"">> - temporary queue with random name will be created.
-spec serve(Callback::rpc_callback()) ->
          {ok, RPCQueue::binary()} | {error, any()}.

serve(Callback) ->
    %% create channel with fair dispatching
    {ok, Channel} = nsm_mq:open([fair_dispatch]),
    serve(Channel, <<"">>, Callback).

-spec serve(RPCQueue::binary(), Callback::rpc_callback()) ->
          {ok, RPCQueue::binary()} | {error, any()}.

serve(RPCQueue, Callback) ->
    %% create channel with fair dispatching
    {ok, Channel} = nsm_mq:open([fair_dispatch]),
    serve(Channel, RPCQueue, Callback).

-spec serve(Channel::channel_pid(), RPCQueue::binary(), Callback::rpc_callback()) ->
          {ok, RPCQueue::binary()} | {error, any()}.

serve(Channel, RPCQueue, Callback) ->
    %% try to consume exclusive
    Options = [{exclusive, true},
               {callback, server_callback(Callback)},
               {state, Channel}],

    %% if queue not exist already it will be created, by default create durable
    %% queue
    %% if RPCQueue = <<"">> random name will be given to queue
    {ok, RPCQueue1} = nsm_mq_channel:create_queue(Channel, RPCQueue,
                                                  [{durable, true},
                                                   {auto_delete, false}]),

    %% if queue already in use we will get exeption
    {ok, _CTag} = nsm_mq_channel:consume(Channel, RPCQueue, Options),

    {ok, RPCQueue1}.


%%
%% Local Functions
%%

server_callback(Callback) ->
    %% channel will be passed as state of the callback
    fun(Msg, Channel) ->
            #envelope{payload = Request, props = Props} = Msg,
            ReplyTo = Props#msg_props.reply_to,
            CorrId  = Props#msg_props.correlation_id,
            Reply =  Callback(Request),
            ok = reply({Channel, ReplyTo, CorrId}, Reply)
    end.

reply({Channel, ReplyTo, CorrId}, Reply) ->
    %% publish reply to client queue
    nsm_mq_channel:publish(Channel, <<"">>, ReplyTo, Reply,
                           [{correlation_id, CorrId}]).
