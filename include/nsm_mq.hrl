%-ifndef(NSM_MQ).
%-define(NSM_MQ, true).

%% notification exchange. Topic exchange to make all system notifications
%% and background works.
-define(NOTIFICATIONS_EX, <<"ns.notifications.topic">>).

%% broadcast exchange to publish user activity: entries changes, comments
%% likes etc.
-define(USER_EVENTS_EX, <<"ns.user_events.topic">>).

%% exchange for db operations. Messages from this exchange go directly into
%% database. Should be used for payment operations.
-define(DB_EX, <<"ns.db.topic">>).


%% Group queue to process db update requests by multiple workers
-define(DB_WORKERS_QUEUE, <<"ns.db.collective.write">>).
%% Routing key workers queue bind to exchange with
-define(DB_WORKERS_COLLECTIVE_KEY, <<"collective.write">>).

%% prefix to add to node names to get exlusive queues on per node basis.
%% Fro example <<"exclusive.mynode@mydomain">>.
-define(DB_WORKERS_EXCLUSIVE_PREFIX, "exclusive.").

%% message envelope
-record(envelope, {payload,
                   exchange,
                   routing_key,
                   consumer_tag,
                   props}).

%% additional messages properties. Used for example in rpc.
-record(msg_props, {reply_to,
                    correlation_id}).


%%
%% Types definitions
%%

-type channel_pid() :: pid().
-type consumer_callback() :: function() | {Mod::atom(), Fun::atom()}.
-type consumer_tag() :: binary().
-type envelope() :: #envelope{}.
-type handler_response() :: ok | reject.
-type routing_key() :: binary().

%-endif.
