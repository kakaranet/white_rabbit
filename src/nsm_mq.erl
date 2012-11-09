-module(nsm_mq).
-author('Vladimir Baranov <baranoff.vladimir@gmail.com>').

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------

-include_lib("amqp_client/include/amqp_client.hrl").
-include("nsm_mq.hrl").

%% --------------------------------------------------------------------
%% External exports
-export([start_link/0]).

-export([open/1,
         node_name/0,

         publish/3,
         publish/4
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {connection,
                channel,     %% this channel should be used only for publishing
                             %% to static exchanges
                node}).

-define(STATIC_EXCHANGES, [?USER_EVENTS_EX, ?DB_EX, ?NOTIFICATIONS_EX]).

%% ====================================================================
%% External functions
%% ====================================================================

open(Options) when is_list(Options) ->
    gen_server:call(?MODULE, {open, Options, self()}).

%% @doc Use publish with default channel, this API should be used for publishing
%% to static exchanges and for clients that have no own process to strore channel
%% identificator

publish(Exchange, RoutingKey, Payload) ->
    publish(Exchange, RoutingKey, Payload, []).

publish(Exchange, RoutingKey, Payload, Options) ->
    gen_server:call(?MODULE, {publish, Exchange, RoutingKey, Payload, Options}).

node_name() ->
    gen_server:call(?MODULE, node_name).

%% ====================================================================
%% Server functions
%% ====================================================================
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% --------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%% --------------------------------------------------------------------
init([]) ->
    process_flag(trap_exit, true),
%    ?INFO("applictions nsm_mq starting..."),

    Host     = nsm_mq_lib:get_env(amqp_host, "localhost"),
    Port     = nsm_mq_lib:get_env(amqp_port, 5672),
    UserStr  = nsm_mq_lib:get_env(amqp_user, "guest"),
    PassStr  = nsm_mq_lib:get_env(amqp_pass, "guest"),
    VHostStr = nsm_mq_lib:get_env(amqp_vhost, "/"),

%    ?INFO("parameters: Host=~s, Port=~p, User=~s, Pass=~s, VHost=~s",
%          [Host, Port, UserStr, PassStr, VHostStr]),

    User  = list_to_binary(UserStr),
    Pass  = list_to_binary(PassStr),
    VHost = list_to_binary(VHostStr),


    %% node name - for now is sting identifier, unique for every node
    %% if not set - node name will be used
    NodeName = nsm_mq_lib:get_env(node, atom_to_list(node())),

    %% if node name is not specified - stop
    NodeName == node_undefined andalso throw({stop, node_undefined}),

    ConnectionOptions =   #amqp_params_network{host = Host,
                                               port = Port,
                                               username = User,
                                               password = Pass,
                                               virtual_host = VHost},

    case catch amqp_connection:start(ConnectionOptions) of
        {ok, Connection} ->

            {ok, Ch} = nsm_mq_channel_sup:start_channel(Connection,
                                                        [{consumer, self()}]),

            %% init static exchanges and queues
            ok = init_static_entries(Ch, NodeName),

            %% start monitor connection and channel
            _MonRef1 = erlang:monitor(process, Connection),
            _MonRef2 = erlang:monitor(process, Ch),

            {ok, #state{connection = Connection,
                        channel = Ch,
                        node = NodeName}};
        {error, Reason} ->
            {stop, {unable_connect_to_broker, Reason}}
    end.

%% --------------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------

handle_call({open, Options, DefaultConsumer}, _From,
            #state{connection = Connection} = State) ->
    Consumer = nsm_mq_lib:opt(consumer, Options, DefaultConsumer),
    Options1 = nsm_mq_lib:override_opt(consumer, Consumer, Options),

    Reply = nsm_mq_channel_sup:start_channel(Connection,  Options1),
    {reply, Reply, State};

handle_call(node_name, _From,  #state{node = Node} = State) ->
    {reply, {ok, Node}, State};

handle_call({publish, Exchange, RoutingKey, Payload, Options}, _From,
            #state{channel = Channel} = State) ->
    Reply = nsm_mq_channel:publish(Channel, Exchange, RoutingKey,
                                   Payload, Options),
    {reply, Reply, State}.

%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast(_Info, State) ->
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------

handle_info({'DOWN', _MonitorRef, process, Connection, Reason},
            #state{connection = Connection} = State) ->
    error_logger:error_msg("connection closed: ~p. Reason: ~p", [Connection, Reason]),
    %% we should restart
    {stop, connection_to_broker_failed, State};
handle_info({'DOWN', _MRef, process, Channel, Reason},
            #state{connection = Connection, channel = Channel} = State) ->
    error_logger:warning_msg("channel ~p closed with reason: ~p",
             [Channel, Reason]),
    %% default channel is down. Start new channel.
    {ok, NewChannel} =
        nsm_mq_channel_sup:start_channel(Connection, [{consumer, self()}]),

    {noreply, State#state{channel = NewChannel}};
handle_info(_Unexpected, State) ->
    {noreply, State}.



%% --------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% --------------------------------------------------------------------
terminate(_Reason, #state{connection = Connection}) ->
    catch amqp_connection:close(Connection),
    ok.

%% --------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% --------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @doc Create  static exhchanges,  queues and bindins.
-spec init_static_entries(channel_pid(), string()) -> ok.

init_static_entries(Ch, Node) ->
    %% create static exchanges if not exists
    ExOptions = [durable, {auto_delete, false}],
    %% add tag to find errors faster
    [{Ex, ok} = {Ex, nsm_mq_channel:create_exchange(Ch, Ex, ExOptions)}
                || Ex <- ?STATIC_EXCHANGES],

    %% create db queue,  durable and non exclusive to make possible
    %% for multiple workers join to queue and process requests
    QOptions = [durable, {exclusive, false}, {auto_delete, false}],
    {ok, Q} = nsm_mq_channel:create_queue(Ch, ?DB_WORKERS_QUEUE, QOptions),

    %% Q == ?DB_WORKERS_QUEUE
    ok = nsm_mq_channel:bind_queue(Ch, Q, ?DB_EX, ?DB_WORKERS_COLLECTIVE_KEY),

    %% create exclusive queue for sequential deliver
    SeqQueue = nsm_mq_lib_db:seq_queue_name(Node),
    SeqRoutingKey = nsm_mq_lib_db:seq_routing_key(Node),

    {ok, SeqQueue} = nsm_mq_channel:create_queue(Ch, SeqQueue, QOptions),
    ok = nsm_mq_channel:bind_queue(Ch, SeqQueue, ?DB_EX, SeqRoutingKey).
