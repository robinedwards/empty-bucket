#!/usr/bin/env escript
-module(empty_bucket).
-author('robin.ge@gmail.com').

option_spec_list()->
    [
        {host,    $h,      "host",    {string, "localhost"}, "pbc host"},
        {port,    $p,      "port",    integer,               "pbc port"},
        {bucket,  $b,      "bucket",  string,                 "bucket"}
    ].

main([]) ->
    getopt:usage(option_spec_list(), escript:script_name());

main(Args)->
    case getopt:parse(option_spec_list(), Args) of
        {ok, {Options, _}} ->
            connect_to_server(Options);

        {error, {Reason, Data}} ->
            io:format("Error: ~s ~p~n~n", [Reason, Data]),
            getopt:usage(option_spec_list(), escript:script_name())
    end,
    halt(1).

connect_to_server(Options) ->
    [{host,Host}, {port,Port},{bucket,Bucket}] = Options,
    io:format("Connecting to: ~s:~p~n", [Host, Port]),

    {ok, Pid} = riakc_pb_socket:start_link(Host, Port),

    case riakc_pb_socket:ping(Pid) of
        pong -> 
           io:format("Connected!~n");
        _ ->
           io:format("Couldn't connect to server.~n"),
           halt(1)
    end,

    empty_bucket(Pid, Bucket).    

empty_bucket(Pid, BucketToDelete) ->
    io:format("Emptying bucket '~s'~n", [BucketToDelete]),
    
    GetBucketKeyPairs = fun (Obj, undefined, none) ->
        [{riak_object:bucket(Obj),riak_object:key(Obj)}]
    end,

    DeleteObjects = fun (List, _Any) ->
        {ok, C} = riak:local_client(),
        Delete = fun (Bucket, Key) ->
            case C:delete(Bucket, Key, 0) of
                  ok -> 1;
                  _ -> 0
            end
        end,
        
        F = fun(Elem, Acc) ->
            case Elem of
              {{Bucket, Key}, _KeyData} ->
                Acc + Delete(Bucket, Key);
              {Bucket, Key} ->
                Acc + Delete(Bucket, Key);
              [Bucket, Key] ->
                Acc + Delete(Bucket, Key);
              _ ->
                Acc + Elem
            end
        end,

        [lists:foldl(F, 0, List)]
    end,

    Query = [
       {map, {qfun, GetBucketKeyPairs}, none, false},
       {reduce, {qfun, DeleteObjects}, none, true}
    ],

    io:format("About to execute query~n"),
    Resp = riakc_pb_socket:mapred_bucket(Pid, list_to_binary(BucketToDelete), Query),
    
    io:format("Response:~p~n~n", [Resp]).
