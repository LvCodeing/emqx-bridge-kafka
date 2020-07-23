%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(emqx_bridge_kafka).

-include("emqx_bridge_kafka.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-export([ register_metrics/0
        , load/1
        , unload/0
        ]).

%% Hooks functions
-export([ 
        on_session_created/3,
        on_session_terminated/4,
        on_message_publish/2
        ]).

register_metrics() ->
    [emqx_metrics:new(MetricName) || MetricName <- ['bridge.kafka.connected', 'bridge.kafka.disconnected', 'bridge.kafka.publish']].


brod_load(_Env) ->
    {ok, _} = application:ensure_all_started(gproc),
    {ok, _} = application:ensure_all_started(brod),
    {ok, Kafka} = application:get_env(?MODULE, bridges),

    KafkaBootstrapHost = proplists:get_value(bootstrap_broker_host, Kafka),
    KafkaBootstrapPort = proplists:get_value(bootstrap_broker_port, Kafka),
    KafkaBootstrapEndpoints = [{KafkaBootstrapHost, KafkaBootstrapPort}], 
    ClientConfig = [{auto_start_producers, true}, {default_producer_config, []}, {reconnect_cool_down_seconds, 10}, {reconnect_cool_down_seconds, 10}],

    ok = brod:start_client(KafkaBootstrapEndpoints, brod_client, ClientConfig),
    ok = brod:start_producer(brod_client, <<"device-status">>, _ProducerConfig = []),
    ok = brod:start_producer(brod_client, <<"device-data">>, _ProducerConfig = []),
    ok = brod:start_producer(brod_client, <<"user-status">>, _ProducerConfig = []),
    ok = brod:start_producer(brod_client, <<"user-ctl">>, _ProducerConfig = []).

%%    emqx_metrics:inc('bridge.kafka.connected').

brod_unload() ->
%%  application:stop(gproc),
  brod:stop_client(brod_client),
  application:stop(brod),

%%    emqx_metrics:inc('bridge.kafka.disconnected'),
  io:format("unload brod~n"),
  ok.

%%getPartition(Key) ->
%%    {ok, Kafka} = application:get_env(?MODULE, bridges),
%%    PartitionNum = proplists:get_value(producer_partition, Kafka),
%%    <<_Fix:120, Match:8>> = crypto:hash(md5, Key),
%%    abs(Match) rem PartitionNum.

produce_kafka_message(Topic,Key, Message, _Env) ->
%%    Key = iolist_to_binary(ClientId),
%%    Partition = getPartition(Key),
%%  emqx_logger:info("~n ~p[~p] =====> Topic:~p, ~n", [?MODULE,?LINE,Topic]),
%%  emqx_logger:info("~n ~p[~p] =====> Message:~p, ~n", [?MODULE,?LINE,Message]),

  Msg = jsx:encode(Message),

    try
      brod:produce_sync(brod_client, Topic, 0, Key, Msg)
%%      emqx_metrics:inc('bridge.kafka.publish')
    catch
        Err ->
          emqx_logger:error("kafka exception:~p ~n",[Err]),
          ok
    end .


%% Called when the plugin application start
load(Env) ->
    brod_load([Env]),
    % emqx:hook('client.authenticate', fun ?MODULE:on_client_authenticate/2, [Env]),
    % emqx:hook('client.check_acl', fun ?MODULE:on_client_check_acl/5, [Env]),
%%    emqx:hook('client.connected', fun ?MODULE:on_client_connected/4, [Env]),
%%    emqx:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]),
%%    emqx:hook('client.subscribe', fun ?MODULE:on_client_subscribe/3, [Env]),
%%    emqx:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/3, [Env]),
    emqx:hook('session.created', fun ?MODULE:on_session_created/3, [Env]),
%%    emqx:hook('session.resumed', fun ?MODULE:on_session_resumed/3, [Env]),
%%    emqx:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [Env]),
%%    emqx:hook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4, [Env]),
    emqx:hook('session.terminated', fun ?MODULE:on_session_terminated/4, [Env]),
    emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
%%    emqx:hook('message.deliver', fun ?MODULE:on_message_deliver/3, [Env]),
%%    emqx:hook('message.acked', fun ?MODULE:on_message_acked/3, [Env]),
%%    emqx:hook('message.dropped', fun ?MODULE:on_message_dropped/3, [Env]).
    ok.

%%Wifi设备上线
on_session_created(#{clientid := <<"w-",DeviceId:20/binary>>,username := <<ProductKey:6/binary,"-",DeviceId:20/binary>>}, _SessInfo, _Env) ->

  Message = [{action, device_status},
             {productKey, ProductKey},
             {deviceId, DeviceId},
             {type, <<"Wifi">>},
             {status, connected},
             {node, node()}],

  produce_kafka_message(<<"device-status">>, DeviceId, Message, _Env);

%%Bluetooth设备上线
on_session_created(#{clientid := <<"b-",DeviceId:20/binary>>,username := <<ProductKey:6/binary,"-",DeviceId:20/binary>>}, _SessInfo, _Env) ->

  Message = [{action, device_status},
    {productKey, ProductKey},
    {deviceId, DeviceId},
    {type, <<"Bluetooth">>},
    {status, connected},
    {node, node()}],

  produce_kafka_message(<<"device-status">>, DeviceId, Message, _Env);

%%IOS用户上线
on_session_created(#{clientid := <<"i-",_:32/binary>>,username := <<UserId:20/binary>>},_SessInfo, _Env) ->

  Message = [{action, user_status},
             {userId, UserId},
             {os, <<"ios">>},
             {status, connected},
             {node, node()}],

  produce_kafka_message(<<"user-status">>, UserId, Message, _Env);

%%安卓用户上线
on_session_created(#{clientid := <<"a-",_:32/binary>>,username := <<UserId:20/binary>>},_SessInfo, _Env) ->
  
  Message = [{action, <<"user-status">>},
             {userId, UserId},
             {os, <<"android">>},
             {status, connected},
             {node, node()}],

  produce_kafka_message(<<"user-status">>, UserId, Message, _Env).

on_session_terminated(#{clientid := <<"w-",DeviceId:20/binary>>, username := <<ProductKey:6/binary,"-",DeviceId:20/binary>>}, Reason,_SessInfo, _Env) ->

  Message = [{action, <<"device-status">>},
             {productKey, ProductKey},
             {deviceId, DeviceId},
             {type, <<"Wifi">>},
             {status, disconnected},
             {node, node()},
             {reason, element(2,Reason)}],

  produce_kafka_message(<<"device-status">>, DeviceId, Message, _Env),

  ok;

on_session_terminated(#{clientid := <<"b-",DeviceId:20/binary>>, username := <<ProductKey:6/binary,"-",DeviceId:20/binary>>}, Reason,_SessInfo, _Env) ->

  Message = [{action, <<"device-status">>},
    {productKey, ProductKey},
    {deviceId, DeviceId},
    {type, <<"Bluetooth">>},
    {status, disconnected},
    {node, node()},
    {reason, element(2,Reason)}],

  produce_kafka_message(<<"device-status">>, DeviceId, Message, _Env),

  ok;

on_session_terminated(#{clientid := <<"i-",_:32/binary>>, username := <<UserId:20/binary>>}, Reason,_SessInfo, _Env) ->

  Message = [{action, <<"user-status">>},
             {userId, UserId},
             {os, ios},
             {status, disconnected},
             {node, node()},
             {reason, element(2,Reason)}],

  produce_kafka_message(<<"user-status">>, UserId, Message, _Env);

on_session_terminated(#{clientid := <<"a-",_:32/binary>>, username := <<UserId:20/binary>>}, Reason,_SessInfo, _Env) ->

  Message = [{action, <<"user-status">>},
             {userId, UserId},
             {os, android},
             {status, disconnected},
             {node, node()},
             {reason, element(2,Reason)}],

  produce_kafka_message(<<"user-status">>, UserId, Message, _Env);

on_session_terminated(_ ,_ , _Reason, _Env) ->
  ok.

%% Transform message and return

on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message = #message{topic = <<"/device/data/down/",ProductKey:6/binary,"/",DeviceId:20/binary>>}, _Env) ->

  Headers = Message#message.headers,

  case maps:is_key(username,Headers) of
    false ->
      Msg = [{action, <<"user-ctl">>},
             {userId, <<"third">>},
             {productKey,ProductKey},
             {deviceId,DeviceId},
             {payload, Message#message.payload},
             {node, node()}],

      produce_kafka_message(<<"user-ctl">>, <<"third">>, Msg, _Env),

      {ok, Message};
    true ->
        UserId = map_get(username,Headers),
        Msg = [{action, <<"user-ctl">>},
               {userId, UserId},
               {productKey,ProductKey},
               {deviceId,DeviceId},
               {payload, Message#message.payload},
               {node, node()}],

        produce_kafka_message(<<"user-ctl">>, UserId, Msg, _Env),

        {ok, Message}
  end;

on_message_publish(Message = #message{topic = <<"/app/data/up/",ProductKey:6/binary,"/",DeviceId:20/binary>>}, _Env) ->

  Msg = [{action, <<"device-data">>},
         {productKey,ProductKey},
         {deviceId,DeviceId},
         {payload, Message#message.payload},
         {node, node()}],

  produce_kafka_message(<<"device-data">>, DeviceId, Msg, _Env),

  {ok, Message};

on_message_publish(_Message, _Env) ->
  io:format("Publish ~s~n", [emqx_message:format(_Message)]),
  {ok, _Message}.


%% Called when the plugin application stop
unload() ->
  brod_unload(),
%%    emqx:unhook('client.authenticate', fun ?MODULE:on_client_authenticate/2),
%%    emqx:unhook('client.check_acl', fun ?MODULE:on_client_check_acl/5),
%%    emqx:unhook('client.connected', fun ?MODULE:on_client_connected/4),
%%    emqx:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
%%    emqx:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/3),
%%    emqx:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/3),
    emqx:unhook('session.created', fun ?MODULE:on_session_created/3),
%%    emqx:unhook('session.resumed', fun ?MODULE:on_session_resumed/3),
%%    emqx:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/4),
%%    emqx:unhook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4),
    emqx:unhook('session.terminated', fun ?MODULE:on_session_terminated/3),
    emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2),
%%    emqx:unhook('message.deliver', fun ?MODULE:on_message_deliver/3),
%%    emqx:unhook('message.acked', fun ?MODULE:on_message_acked/3),
%%    emqx:unhook('message.dropped', fun ?MODULE:on_message_dropped/3).
    ok.
