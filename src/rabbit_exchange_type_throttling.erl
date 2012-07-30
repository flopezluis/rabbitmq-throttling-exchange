%% 
%%
%% This plugin is based on rabbitmq-recent-history-exchange
%%  from Alvaro Videla https://github.com/videlalvaro/rabbitmq-recent-history-exchange
%%
%% This is plugin has been developed by ShuttleCloud.
%% 
%% This exchange gives you the possibility to set throttling to any 
%% exchange. This exchange receives a message and after a time it's delivered
%% to the final exchange. It works as an intermediary
%%
%%  You should set this headers:
%%      - to_exchange:           The final exchange
%%      - messages_per_second:   The rate of messages in seconds.
%% 
%%  For example:
%%      - to_exchange= services
%%      - messages_per_second: 0.017
%%
%%      Delivers a message every 60 seconds to the exchange services.
%%
%%  This plugin doesn't accomplish the standar erlang convention and 
%%  It's very unstable.
%%  Take into account that I'm not an erlang programmer nor rabbitmq committer, 
%%  I appreciate all reviews and feedback.

-module(rabbit_exchange_type_throttling).
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, create/2, delete/3, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).
-export([setup_schema/0]).

-rabbit_boot_step({rabbit_exchange_type_rh_registry,
[{description, "throttling exchange type: registry"},
  {mfa, {rabbit_registry, register,
          [exchange, <<"x-throttling">>,
           ?MODULE]}},
  {requires, rabbit_registry},
  {enables, kernel_ready}]}).

-rabbit_boot_step({rabbit_exchange_type_rh_mnesia,
  [{description, "throttling exchange type: mnesia"},
    {mfa, {?MODULE, setup_schema, []}},
    {requires, database},
    {enables, external_infrastructure}]}).

-define(RH_TABLE, rh_exchange_throttling_table).
-record(lastSent, {key, timestamp}).

description() ->
  [{name, <<"throttling">>},
   {description, <<"It adds throttling.">>}].

serialise_events() -> false.

current_time_ms() ->
    {Mega,Sec,Micro} = erlang:now(),
    ((Mega*1000000+Sec)*1000000+Micro)/1000.

extract_header(Headers, Key) ->
    Found = lists:keyfind(Key, 1, Headers),
    {_,_,Header} = Found,
    Header.

route(#exchange{name = XName}, Delivery) ->
  BasicMessage = (Delivery#delivery.message),
  Content = (BasicMessage#basic_message.content),
  Headers = rabbit_basic:extract_headers(Content),
  [RoutingKey|_] = BasicMessage#basic_message.routing_keys,
  ToExchange = extract_header(Headers, <<"to_exchange">>),
  %Get Last sent from Db
  LastTime = get_msgs_from_cache(ToExchange),
  if 
     %First message sent
     LastTime == [] ->
        TimeToNextSent = 0;
     true ->
        MsgPerSecondStr = extract_header(Headers, <<"messages_per_second">>),
        {MsgPerSecond, _} = string:to_float(binary_to_list(MsgPerSecondStr)),
        MilisecondsBetweenMsg = 1000 / MsgPerSecond,
        Now = current_time_ms(),
        Elapsed = Now - LastTime,
        ValueTmp = MilisecondsBetweenMsg - Elapsed,
        if 
          ValueTmp < 0 -> TimeToNextSent = 0;
          true -> TimeToNextSent = round(ValueTmp)
        end
  end,
  %% TODO may I also store by routing key? and I should update not add 
  cache_msg(ToExchange, current_time_ms() + TimeToNextSent),
  {Ok, Msg} = rabbit_basic:message({resource,<<"/">>, exchange, ToExchange}, RoutingKey, Content),
  NewDelivery = build_delivery(Delivery, Msg),
  Pid = spawn(fun () -> deliver_message(TimeToNextSent, NewDelivery) end),
  [].

validate(_X) -> ok.
create(_Tx, _X) -> ok.

deliver_message(Timeout, Delivery) ->
    %%It delivers the message after the timeout
    receive 
    after 
        Timeout ->
          rabbit_basic:publish(Delivery),
          ok
    end.

build_delivery(Delivery, Message) ->
    %%Build a Delivery from other delivery
    Mandatory = Delivery#delivery.mandatory,
    Immediate = Delivery#delivery.immediate,
    MsgSeqNo = Delivery#delivery.msg_seq_no,
    NewDelivery = rabbit_basic:delivery(Mandatory, Immediate, Message, MsgSeqNo),
    NewDelivery.

delete(_Tx, #exchange{ name = XName }, _Bs) ->
  rabbit_misc:execute_mnesia_transaction(
    fun() ->
      mnesia:delete(?RH_TABLE, XName, write)
    end),
  ok.

add_binding(_Tx, #exchange{ name = XName },
            #binding{ destination = QName }) ->
  ok.

remove_bindings(_Tx, _X, _Bs) -> ok.

assert_args_equivalence(X, Args) ->
  rabbit_exchange:assert_args_equivalence(X, Args).

setup_schema() ->
  case mnesia:create_table(?RH_TABLE,
          [{attributes, record_info(fields, lastSent)},
           {record_name, lastSent},
           {type, set}]) of
      {atomic, ok} -> ok;
      {aborted, {already_exists, ?RH_TABLE}} -> ok
  end.

%%private
cache_msg(XName, Timestamp) ->
  rabbit_misc:execute_mnesia_transaction(
    fun () ->
      store_msg(XName, Timestamp)
    end).

get_msgs_from_cache(XName) ->
  rabbit_misc:execute_mnesia_transaction(
    fun () ->
      case mnesia:read(?RH_TABLE, XName) of
        [] ->
          [];
        [#lastSent{key = XName, timestamp=LastSent}] ->
          LastSent
      end
    end).

store_msg(Key, Timestamp) ->
  mnesia:write(?RH_TABLE,
    #lastSent{key     = Key,
            timestamp = Timestamp},
    write).

msgs_from_content(XName, Cached) ->
  lists:map(
    fun(Content) ->
        {Props, Payload} = rabbit_basic:from_content(Content),
        rabbit_basic:message(XName, <<"">>, Props, Payload)
    end, Cached).

deliver_messages(Queue, Msgs) ->
  lists:map(
    fun (Msg) ->
      Delivery = rabbit_basic:delivery(false, false, Msg, undefined),
      rabbit_amqqueue:deliver(Queue, Delivery)
    end, lists:reverse(Msgs)).

queue_not_found_error(QName) ->
  rabbit_misc:protocol_error(
    internal_error,
    "could not find queue '~s'",
    [QName]).
