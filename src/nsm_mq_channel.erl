-module(nsm_mq_channel).
-author('Vladimir Baranov <baranoff.vladimir@gmail.com>').

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("nsm_mq.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%% --------------------------------------------------------------------
%% External exports
-export([start_link/2]).

-export([create_exchange/3,
         delete_exchange/2,

         create_queue/3,
         delete_queue/2,

         consume/3,
         cancel_consume/2,

         bind_queue/4,
         bind_exchange/4,

         unbind_queue/4,
         unbind_exchange/4,

         publish/4,
         publish/5,

         ack/2,

         close/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(CALLBACK_CONSUMER(Pid), {amqp_direct_consumer, [Pid]}).
-define(CONFIRM_MODE, confirm).
-define(NORMAL_MODE, normal).

-record(state, {channel,
                monitors = dict:new(),
                mode = ?NORMAL_MODE :: confirm | normal,
                consumers = dict:new() :: dict(),
                current_confirm_no,
                returned = none,
                publishes = dict:new()}).

-record(consumer, {tag, pid, callback, state}).

%% ====================================================================
%% External functions
%% ====================================================================

start_link(Connection, Consumer) ->
    gen_server:start_link(?MODULE, [Connection, Consumer], []).

create_exchange(Channel, ExchangeName, Options) ->
    gen_server:call(Channel, {create_exchange, ExchangeName, Options}, infinity).

delete_exchange(Channel, ExchangeName) ->
    gen_server:call(Channel, {delete_exchange, ExchangeName}).

create_queue(Channel, QueueName, Options) ->
    gen_server:call(Channel, {create_queue, QueueName, Options}, infinity).

consume(Channel, Queue, Options) ->
    %% add pid of the caller as consumer is wasn't specified
    Consumer =  nsm_mq_lib:opt(consumer, Options, self()),
    Options1 = nsm_mq_lib:override_opt(consumer, Consumer, Options),
    gen_server:call(Channel, {consume, Queue, Options1}, infinity).

cancel_consume(Channel, ConsumerTag) ->
    gen_server:call(Channel, {cancel_consume, ConsumerTag}).

delete_queue(Channel, QueueName) ->
    gen_server:call(Channel, {delete_queue, QueueName}).

bind_queue(Channel, QueueName, Exchange, RoutingKey) ->
    gen_server:call(Channel, {bind_queue, QueueName, Exchange, RoutingKey}, infinity).

bind_exchange(Channel, Exchange1, Exchange2, RoutingKey) ->
    gen_server:call(Channel, {bind_exchange, Exchange1, Exchange2, RoutingKey}, infinity).

unbind_queue(Channel, QueueName, Exchange, RoutingKey) ->
    gen_server:call(Channel, {unbind_queue, QueueName, Exchange, RoutingKey}).

unbind_exchange(Channel, Exchange1, Exchange2, RoutingKey) ->
    gen_server:call(Channel, {unbind_exchange, Exchange1, Exchange2, RoutingKey}).

publish(Channel, Exchange, RoutingKey, Payload) ->
    publish(Channel, Exchange, RoutingKey, Payload, []).

publish(Channel, Exchange, RoutingKey, Payload, Options) ->
    gen_server:call(Channel, {publish, Exchange, RoutingKey, Payload, Options}).


close(Channel) ->
    gen_server:cast(Channel, close).

ack(Channel, DeliveryTag) ->
    gen_server:cast(Channel, {ack, DeliveryTag}).


%% ====================================================================
%% Server functions
%% ====================================================================

%% --------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%% --------------------------------------------------------------------
init([Connection, Options]) ->
    process_flag(trap_exit, true),
    Consumer = nsm_mq_lib:opt(consumer, Options, default_consumer_undefined),
    FairDispatch = nsm_mq_lib:opt(fair_dispatch, Options, false),
    Confirm = nsm_mq_lib:opt(confirm_mode, Options, false),

    case amqp_connection:open_channel(Connection, none, ?CALLBACK_CONSUMER(self())) of
        {ok, Pid} ->
            %%FIXME: change link to monitor
            link(Consumer),
            link(Pid),
            FairDispatch andalso tune_channel(Pid, fair_dispatch),

            State1 =
                case Confirm of
                    true ->
                        do_confirm_mode(Pid),
                        #state{mode= ?CONFIRM_MODE,current_confirm_no = 1};
                    false ->
                        #state{}
                end,

            amqp_channel:register_return_handler(Pid,self()),

            {ok, State1#state{channel = Pid}};

        {error, Reason} ->
            {stop, Reason};

        Other ->
            {stop, Other}
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
handle_call({create_exchange, Exchange, Options}, _From,
            #state{channel = Channel} = State) ->
    {reply, do_create_exchange(Channel, Exchange, Options), State};


handle_call({create_queue, Name, Options}, _From,
            #state{channel = Channel} = State) ->
    %% create queue
    {ok, Queue} = do_create_queue(Channel, Name, Options),
    {reply, {ok, Queue}, State};

handle_call({consume, Queue, Options}, _From,
            #state{channel = Channel,
                   monitors = Monitors,
                   consumers = Consumers} = State) ->
    Pid = nsm_mq_lib:opt(consumer, Options, unreal_to_match_this),
    Callback = nsm_mq_lib:opt(callback, Options, default_callback(Pid)),
    InitState = nsm_mq_lib:opt(state, Options, []),
    Exclusive = nsm_mq_lib:opt(exclusive, Options, false),


    Sub = #'basic.consume'{queue=Queue, exclusive = Exclusive},
    #'basic.consume_ok'{consumer_tag = CTag} =
                           amqp_channel:subscribe(Channel, Sub, self()),

    %% start consume on queue
    ConsumerRec = #consumer{tag = CTag, pid = Pid, callback = Callback,
                            state = InitState},

    Monitors1 = add_to_monitor_dict(Pid, Monitors),

    {reply, {ok, CTag},
     State#state{consumers = dict:store(CTag, ConsumerRec, Consumers),
                 monitors = Monitors1}};

handle_call({delete_exchange, Exchange}, _From,
            #state{channel = Channel} = State) ->
    {reply, do_delete_exchange(Channel, Exchange), State};

handle_call({delete_queue, Queue}, _From,
            #state{channel = Channel} = State) ->
    {reply, do_delete_queue(Channel, Queue), State};

handle_call({cancel_consume, CTag}, _From,
            #state{channel = Channel} = State) ->
    do_cancel_consume(CTag, Channel),
    {reply, ok, State};

handle_call({bind_exchange, Exchange1, Exchange2, RoutingKey}, _From,
            #state{channel = Channel} = State) ->
    {reply, do_bind_exchange(Channel, Exchange1, Exchange2, RoutingKey), State};

handle_call({bind_queue, Queue, Exchange, RoutingKey}, _From,
            #state{channel = Channel} = State) ->
    {reply, do_bind_queue(Channel, Queue, Exchange, RoutingKey), State};

handle_call({unbind_exchange, Exchange1, Exchange2, RoutingKey}, _From,
            #state{channel = Channel} = State) ->
    {reply, do_unbind_exchange(Channel, Exchange1, Exchange2, RoutingKey), State};

handle_call({unbind_queue, Queue, Exchange, RoutingKey}, _From,
            #state{channel = Channel} = State) ->
    {reply, do_unbind_queue(Channel, Queue, Exchange, RoutingKey), State};

handle_call({publish, Exchange, RoutingKey, Payload, Options}, From,
            #state{channel = Channel,
                   current_confirm_no = CCN,
                   mode = ?CONFIRM_MODE,
                   publishes = Pubs} = State) ->
    do_publish(Channel, Exchange, RoutingKey, Payload, Options),

    case nsm_mq_lib:opt(nowait, Options, false) of
        %% client don't want to wait for confirmation
        true ->
            {reply, ok, State#state{current_confirm_no = CCN+1}};
        false ->
            %% reply will be sended when ack or nack will be received from
            %% server
            {noreply, State#state{current_confirm_no = CCN+1,
                                  publishes = dict:store(CCN, From, Pubs)}}
    end;

handle_call({publish, Exchange, RoutingKey, Payload, Options}, _From,
            #state{channel = Channel} = State) ->
    do_publish(Channel, Exchange, RoutingKey, Payload, Options),
    {reply, ok, State};

handle_call(Req, _From, State) ->
    Reply = {not_implemented, Req},
    {reply, Reply, State}.

%% --------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_cast(close, State) ->
    {stop, normal, State};
handle_cast({ack, DeliveryTag}, #state{channel = Channel} = State) ->
    do_ack(Channel, DeliveryTag),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%% --------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%% --------------------------------------------------------------------
handle_info({'EXIT', Channel, Reason}, #state{channel = Channel} = State) ->
      error_logger:error_msg("channel exited: ~p, Reason: ~p", [Channel, Reason]),
      {stop, channel_exited, State};

handle_info({'EXIT', Pid, Reason}, #state{consumers = Consumers,
                                          channel = Channel,
                                          monitors = Monitors} = State) ->
    case dict:find(Pid, Monitors) of
        {ok, _CountMRef} ->
            io:format("EXITED: ~p~n", [Pid]),
            error_logger:error_msg("consumer process exited: ~p, Reason: ~p", [Pid, Reason]),
            %% cancel consuming, cleanup will be performing when receive
            %% consume confirmation from server
            dict:map(fun(_, #consumer{pid = Pid1, tag = CTag})
                          when Pid1 =:= Pid ->
                             do_cancel_consume(CTag, Channel);
                        (_, _) ->
                             ok
                     end, Consumers);
        _ ->
            ok
    end,
    {noreply, State};

handle_info({'DOWN', _MRef, process, Pid, _Info},
                #state{consumers = Consumers,
                       channel = Channel,
                       monitors = Monitors} = State) ->
    error_logger:error_msg("process down: ~p", [Pid]),

    case dict:find(Pid, Monitors) of
        {ok, _CountMRef} ->

            %% cancel consuming, cleanup will be performing when receive
            %% consume confirmation from server
            dict:map(fun(_, #consumer{pid = Pid1, tag = CTag})
                          when Pid1 =:= Pid ->
                             do_cancel_consume(CTag, Channel);
                        (_, _) ->
                             ok
                     end, Consumers),
            {noreply, State};
        error ->
            %% FIXME: add logging
            {noreply, State}
    end;

handle_info(Info, State) ->
    process_message(Info, State).


%% --------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%% --------------------------------------------------------------------
terminate(_Reason, #state{channel = Channel}) ->
    catch do_close(Channel),
    ok.

%% --------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%% --------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------

process_message({#'basic.consume'{consumer_tag = _Tag}, _}, State) ->
    {noreply, State};
process_message(#'basic.consume_ok'{consumer_tag = _Tag}, State) ->
    {noreply, State};
process_message(#'basic.cancel'{consumer_tag = Tag}, State) ->
    State1 = do_cancel(Tag, State),
    case dict:size(State1#state.consumers) of
        %% no consumers left - stop channel
        0 ->
            {stop, normal, State};
        _ ->
            {noreply, State1}
    end;
process_message(#'basic.cancel_ok'{consumer_tag = Tag}, State) ->
    State1 = do_cancel(Tag, State),
    case dict:size(State1#state.consumers) of
        %% no consumers left - stop channel
        0 ->
            {stop, normal, State};
        _ ->
            {noreply, State1}
    end;
process_message({#'basic.deliver'{} = BD,
                 #amqp_msg{props = Props, payload = PL}}, State) ->
    CTag = BD#'basic.deliver'.consumer_tag,
    DTag = BD#'basic.deliver'.delivery_tag,
    Exchange = BD#'basic.deliver'.exchange,
    RoutingKey = BD#'basic.deliver'.routing_key,
    State1 = do_deliver(CTag, DTag, Exchange, RoutingKey, PL, Props, State),
    {noreply, State1};
process_message({#'basic.return'{}, _} = Return, State) ->
    %% basic ack must be sent after basic return. So just save it and wait for
    %% ack.
    {noreply, State#state{returned = Return}};

process_message(#'basic.ack'{delivery_tag = DTag},
                #state{publishes = Pubs,
                       returned = Returned,
                       mode = ?CONFIRM_MODE} = State) ->

    case dict:find(DTag, Pubs) of
        {ok, ReplyTo} ->
            Reply = case Returned of
                        none ->
                            ok;
                        {#'basic.return'{reply_code = RK, reply_text = RT}, _} ->
                            {error, {returned, RK, RT}}
                    end,

            gen_server:reply(ReplyTo, Reply),
            %% erase publish and returned state
            {noreply, State#state{publishes = dict:erase(DTag, Pubs),
                                  returned = none}};
        error ->
            %% not wait for this ack - skip
            {noreply, State}
    end;

process_message(#'basic.nack'{delivery_tag = DTag},
                #state{publishes = Pubs,
                       mode = ?CONFIRM_MODE} = State) ->

    case dict:find(DTag, Pubs) of
        {ok, ReplyTo} ->
            gen_server:reply(ReplyTo, {error, nack}),
            {noreply, State#state{publishes = dict:erase(DTag, Pubs)}};
        error ->
            %% not wait for this ack - skip
            {noreply, State}
    end;

process_message(Unexpected, State) ->
    %% TODO: add logging
    %% TODO: add conmirmation processing
    io:format("UNEXPECTED: ~p~n", [Unexpected]),
    {noreply, State}.


do_close(Channel) ->
    amqp_channel:close(Channel).

tune_channel(Channel, fair_dispatch) ->
    QoS = #'basic.qos'{prefetch_count = 1},
    #'basic.qos_ok'{} = amqp_channel:call(Channel, QoS).

do_create_exchange(Channel, Exchange, Options) ->
    Durable    = nsm_mq_lib:opt(durable, Options, false),
    Passive    = nsm_mq_lib:opt(passive, Options, false),
    Type       = nsm_mq_lib:opt(type, Options, <<"topic">>),
    Autodelete = nsm_mq_lib:opt(auto_delete, Options, true),
    NoWait     = nsm_mq_lib:opt(nowait, Options, false),
    Internal   = nsm_mq_lib:opt(internal, Options, false),

    Declaration = #'exchange.declare'{exchange = Exchange,
                                      type = Type,
                                      durable  = Durable,
                                      auto_delete = Autodelete,
                                      internal = Internal,
                                      passive = Passive,
                                      nowait = NoWait},

    #'exchange.declare_ok'{} = amqp_channel:call(Channel, Declaration),
    ok.

do_delete_exchange(Channel, Exchange) ->
    DeleteExchange = #'exchange.delete'{exchange = Exchange},
    #'exchange.delete_ok'{} = amqp_channel:call(Channel, DeleteExchange),
    ok.

do_create_queue(Channel, Name, Options) ->
    Durable    = nsm_mq_lib:opt(durable, Options, false),
    Autodelete = nsm_mq_lib:opt(auto_delete, Options, true),
    NoWait     = nsm_mq_lib:opt(nowait, Options, false),
    Exclusive  = nsm_mq_lib:opt(exclusive, Options, false),
    Args       = build_queue_declare_arguments(Options),

    Declaration = #'queue.declare'{queue = Name,
                                   durable = Durable,
                                   nowait = NoWait,
                                   auto_delete = Autodelete,
                                   exclusive = Exclusive,
                                   arguments = Args},

    #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Channel, Declaration),
    {ok, Queue}.

do_delete_queue(Channel, Queue) ->
    DeleteQueue = #'queue.delete'{queue = Queue},
    #'queue.delete_ok'{} = amqp_channel:call(Channel, DeleteQueue),
    ok.

do_bind_queue(Channel, Queue, Exchange, RoutingKey) ->
    Bind = #'queue.bind'{exchange = Exchange,
                         queue = Queue,
                         routing_key = RoutingKey},

    #'queue.bind_ok'{} = amqp_channel:call(Channel, Bind),
    ok.

do_bind_exchange(Channel, Exchange1, Exchange2, RoutingKey) ->
    Bind = #'exchange.bind'{destination = Exchange1,
                         source = Exchange2,
                         routing_key = RoutingKey},

    #'exchange.bind_ok'{} = amqp_channel:call(Channel, Bind),
    ok.

do_unbind_queue(Channel, Queue, Exchange, RoutingKey) ->
    Unbind = #'queue.unbind'{exchange = Exchange,
                             queue = Queue,
                             routing_key = RoutingKey},
    #'queue.unbind_ok'{} = amqp_channel:call(Channel, Unbind),
    ok.

do_unbind_exchange(Channel, Exchange1, Exchange2, RoutingKey) ->
    Unbind = #'exchange.unbind'{destination = Exchange1,
                                source = Exchange2,
                                routing_key = RoutingKey},

    #'exchange.unbind_ok'{} = amqp_channel:call(Channel, Unbind),
    ok.

do_deliver(CTag, DTag, Exchange, RoutingKey, PL, Props, State) ->
    Channel = State#state.channel,
    case dict:find(CTag, State#state.consumers) of
        {ok, #consumer{callback = Callback, state = CState}} ->
            spawn_link(
              fun()->
                      R = try
                              DecodedPL = nsm_mq_lib:decode(PL),
                              ReplyTo = Props#'P_basic'.reply_to,
                              CorrId = Props#'P_basic'.correlation_id,
                              MsgProps =  #msg_props{reply_to = ReplyTo,
                                                     correlation_id = CorrId},

                              Envelope = #envelope{consumer_tag = CTag,
                                                   exchange = Exchange,
                                                   payload = DecodedPL,
                                                   routing_key = RoutingKey,
                                                   props = MsgProps},

                              case Callback of
                                  {Mod, Fun} ->
                                      Mod:Fun(Envelope, CState);
                                  Fun when is_function(Fun, 2) ->
                                      Fun(Envelope, CState)
                              end
                          catch
                              _:E->
                                  %% TODO: add logging
                                  %% if error occured - reject message
                                  io:format("Callback crashed: ~p, Stack=~p~n",
                                            [E, erlang:get_stacktrace()]),
                                  {error, E}
                          end,
                      %% XXX: maybe we should do ack right after receive message
                      %% in this case perfomance will be better, but
                      %% messages can be silently lost when handler is failed.
                      case R of
                          ok ->
                              do_ack(Channel, DTag);
                          {ok, _} ->
                              do_ack(Channel, DTag);
                          {error, _} ->
                              %% FIXME: add reject method
                              ok;
                          Other ->
                              exit({unexpected_handler_result, Other})
                      end
              end),

            State;
        error ->
            %% FIXME: add logging
            %% deliver received but no consumer
            io:format("~w: deliver received, but no consumer! ~9999p~n",
                      [?MODULE, {CTag, DTag, Exchange, RoutingKey, PL}]),
            State
    end.


do_cancel(ConsumerTag, #state{consumers = Consumers,
                              monitors = Monitors} = State) ->

    case dict:find(ConsumerTag, Consumers) of
        {ok, #consumer{pid = Pid, callback = _Callback, state = _CState}} ->
            Monitors1 = remove_from_monitor_dict(Pid, Monitors),
            %% TODO: call calback about cancellation
            State#state{monitors = Monitors1,
                        consumers = dict:erase(ConsumerTag, Consumers)};
        error ->
            %% not in consumer list - do nothing
            State
    end.

do_cancel_consume(CTag, Channel) ->
    %% cleanup will be done in process_message/2, when channel receive
    %% confirmation from channel
    Cancel = #'basic.cancel'{consumer_tag = CTag},
    #'basic.cancel_ok'{} = amqp_channel:call(Channel, Cancel),
    ok.

do_publish(Channel, Exchange, RoutingKey, Payload, Options) ->
    Mandatory = nsm_mq_lib:opt(mandatory, Options, false),
    Immediate = nsm_mq_lib:opt(immediate, Options, false),
    ReplyTo   = nsm_mq_lib:opt(reply_to, Options, undefined),
    CorrID    = nsm_mq_lib:opt(correlation_id, Options, undefined),

    Publish = #'basic.publish'{exchange = Exchange,
                               mandatory = Mandatory,
                               immediate = Immediate,
                               routing_key = RoutingKey},

    Durable = nsm_mq_lib:opt(durable, Options, false),
    %% if IsDurable option is set, set delivery mode to 2
    DeliveryMode = if Durable -> 2; true -> 1 end,
    MsgProps = #'P_basic'{delivery_mode = DeliveryMode,
                          reply_to = ReplyTo,
                          correlation_id = CorrID},

    EncodedMsg = #amqp_msg{props = MsgProps,
                           payload = nsm_mq_lib:encode(Payload)},
    amqp_channel:cast(Channel, Publish, EncodedMsg).

%% process mode change
do_confirm_mode(Channel) ->
    Confirm = #'confirm.select'{},
    amqp_channel:register_confirm_handler(Channel, self()),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, Confirm),
    ok.

do_ack(Channel, DeliveryTag) ->
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = DeliveryTag}).


default_callback(Pid) ->
    fun(#envelope{} = Msg, _) ->
            Pid!{message, Msg},
            ok
    end.

add_to_monitor_dict(Pid, Monitors) ->
    case dict:find(Pid, Monitors) of
        error ->
            dict:store(Pid, {1, erlang:monitor(process, Pid)}, Monitors);
        {ok, {Count, MRef}} ->
            dict:store(Pid, {Count + 1, MRef}, Monitors)
    end.

remove_from_monitor_dict(Pid, Monitors) ->
    case dict:fetch(Pid, Monitors) of
        {1, MRef}     -> erlang:demonitor(MRef),
                         dict:erase(Pid, Monitors);
        {Count, MRef} -> dict:store(Pid, {Count - 1, MRef}, Monitors)
    end.

%% to understand what this options can be usefull for,
%% see http://www.rabbitmq.com/extensions.html
build_queue_declare_arguments([]) ->
    [];
%% timeout in miliseconds
build_queue_declare_arguments([{ttl, Value}|Rest]) ->
    %Bin = list_to_binary(integer_to_list(Value)),
    [{<<"x-message-ttl">>, long, Value}|build_queue_declare_arguments(Rest)];
%% where to route dead messages
build_queue_declare_arguments([{dead_letter_exchange, Value}|Rest]) ->
    [{<<"x-dead-letter-exchange">>, longstr, Value}|build_queue_declare_arguments(Rest)];
%% setup routing key for dead messages
build_queue_declare_arguments([{dead_letter_routing_key, Value}|Rest]) ->
    [{<<"x-dead-letter-routing-key">>, longstr, Value}|build_queue_declare_arguments(Rest)];
build_queue_declare_arguments([_|Rest]) ->
    build_queue_declare_arguments(Rest).
