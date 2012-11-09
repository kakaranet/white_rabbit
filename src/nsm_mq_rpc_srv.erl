-module(nsm_mq_rpc_srv).
-author('Vladimir Baranov <baranoff.vladimir@gmail.com>').

-behaviour(gen_server).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------

-include("nsm_mq.hrl").

%% --------------------------------------------------------------------
%% External exports
-export([call/2, call/3]).

%%@private
-export([handle/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/0]).

-record(state, {channel,
                reply_queue,
                clients = dict:new()}). %% {CorrelationId, ReplyTo}


%% default call timeout
-define(DEFAULT_TIMEOUT, 5000).

%% ====================================================================
%% External functions
%% ====================================================================

call(RoutingKey, Request) ->
    call(RoutingKey, Request, ?DEFAULT_TIMEOUT).

call(RoutingKey, Request, Timeout)
  when is_binary(RoutingKey)
           andalso is_integer(Timeout)
           orelse Timeout == infinity ->
    %% TODO: expiration of the messages
    gen_server:call(?MODULE, {call, RoutingKey, Request, Timeout}, Timeout).

%%@private
handle(Msg, HandlerState) ->
    %% use cast for immediately ack
    gen_server:cast(?MODULE, {handle, Msg, HandlerState}).

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
    %% create channel and queue for receiving replies
    {ok, Channel} = nsm_mq:open([]),
    {ok, ReplyQueue} = nsm_mq_channel:create_queue(Channel, <<"">>, []),

    %% start consume on queue
    Options = [{exclusive, true},
               {callback, {?MODULE, handle}}],
    {ok, _CTag} = nsm_mq_channel:consume(Channel, ReplyQueue, Options),

    {ok, #state{channel = Channel,
                reply_queue = ReplyQueue,
                clients = dict:new()}}.


handle_call({call, RoutingKey, Request, Timeout}, From,
            #state{channel = Ch, clients = Clients, reply_queue = RQ} = State) ->

    CorrId = term_to_binary(make_ref()),
    PubOptions = case Timeout of
                     infinity ->
                         [{reply_to, RQ}, {correlation_id, CorrId}];
                     I when is_integer(I) ->
                         [{reply_to, RQ}, {correlation_id, CorrId},
                          {expiration, Timeout}]
                 end,

    %% publish to default exchange with needed routing key
    ok = nsm_mq_channel:publish(Ch, <<"">>, RoutingKey, Request, PubOptions),

    {noreply, State#state{clients = dict:store(CorrId, From, Clients)}}.


handle_cast({handle, Msg, _HandlerState}, #state{clients = Clients} = State) ->

    #envelope{payload = Reply, props = Props} = Msg,
    CorrId  = Props#msg_props.correlation_id,

    case dict:find(CorrId, Clients) of
        error ->
            error_logger:warning_msg("received reply, but no clients: CorrId=~p, Reply=~p",
                     [CorrId, Reply]),
            ok;
        {ok, Client} ->
            gen_server:reply(Client, Reply)
    end,

    {noreply, State#state{clients = dict:erase(CorrId, Clients)}}.


handle_info(Info, State) ->
    error_logger:error_msg("handle info: ~p", [Info]),
    {noreply, State}.


terminate(_Reason, #state{channel = Ch, reply_queue = RQ}) ->
    catch nsm_mq_channel:delete_queue(Ch, RQ),
    catch nsm_mq_channel:close(Ch),
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------

