import grpc

import eventstore_pb2
import eventstore_pb2_grpc

class Client:
    def __init__(self, server):
        self.server = server
        self.channel = grpc.insecure_channel(self.server)
        
        self.KV.stub = eventstore_pb2_grpc.KVStub(self.channel)
        self.Map.stub = eventstore_pb2_grpc.MapStub(self.channel)
        self.Queue.stub = eventstore_pb2_grpc.QueueStub(self.channel)
    
    def Close(self):
        self.channel.Close()

    class KV:
        def Set(self, key, value, **kwargs):
            request = eventstore_pb2.SetKVRequest(
                location = eventstore_pb2.LocationType(
                    scope = eventstore_pb2.ScopeType(
                        type = kwargs.get("scope", eventstore_pb2.Global),
                        bridge = kwargs.get("bridge", None),
                        instance = kwargs.get("instance", None)
                    ),
                    key = key
                ),
                ttl = kwargs.get("ttl", None),
                value = value
            )
            resp = self.stub.Set(request)
            return resp

        def Get(self, key, **kwargs):
            request = eventstore_pb2.GetKVRequest(
                location = eventstore_pb2.LocationType(
                    scope = eventstore_pb2.ScopeType(
                        type = kwargs.get("scope", eventstore_pb2.Global),
                        bridge = kwargs.get("bridge", None),
                        instance = kwargs.get("instance", None)
                    ),
                    key = key
                )
            )
            resp = self.stub.Get(request)
            return resp
    
    class Map:
        def New(self, key, **kwargs):
            request = eventstore_pb2.NewMapRequest(
                location = eventstore_pb2.LocationType(
                    scope = eventstore_pb2.ScopeType(
                        type = kwargs.get("scope", eventstore_pb2.Global),
                        bridge = kwargs.get("bridge", None),
                        instance = kwargs.get("instance", None)
                    ),
                    key = key
                ),
                ttl = kwargs.get("ttl", None),
            )

        def Set(self, key, value, field, **kwargs):
            request = eventstore_pb2.SetMapFieldRequest(
                location = eventstore_pb2.LocationType(
                    scope = eventstore_pb2.ScopeType(
                        type = kwargs.get("scope", eventstore_pb2.Global),
                        bridge = kwargs.get("bridge", None),
                        instance = kwargs.get("instance", None)
                    ),
                    key = key
                ),
                field = field
                value = value
            )
            resp = self.stub.Set(request)
            return resp

        def Get(self, key, field, **kwargs):
            request = eventstore_pb2.GetMapFieldRequest(
                location = eventstore_pb2.LocationType(
                    scope = eventstore_pb2.ScopeType(
                        type = kwargs.get("scope", eventstore_pb2.Global),
                        bridge = kwargs.get("bridge", None),
                        instance = kwargs.get("instance", None)
                    ),
                    key = key
                ),
                field = field
            )
            resp = self.stub.Get(request)
            return resp

    class Queue:
        def New(self, key, **kwargs):
            request = eventstore_pb2.NewQueueRequest(
                location = eventstore_pb2.LocationType(
                    scope = eventstore_pb2.ScopeType(
                        type = kwargs.get("scope", eventstore_pb2.Global),
                        bridge = kwargs.get("bridge", None),
                        instance = kwargs.get("instance", None)
                    ),
                    key = key
                ),
                ttl = kwargs.get("ttl", None),
            )

        def Push(self, key, value, **kwargs):
            request = eventstore_pb2.PushQueueRequest(
                location = eventstore_pb2.LocationType(
                    scope = eventstore_pb2.ScopeType(
                        type = kwargs.get("scope", eventstore_pb2.Global),
                        bridge = kwargs.get("bridge", None),
                        instance = kwargs.get("instance", None)
                    ),
                    key = key
                ),
                value = value
            )
            resp = self.stub.Set(request)
            return resp

        def Pop(self, key, **kwargs):
            request = eventstore_pb2.PopQueueRequest(
                location = eventstore_pb2.LocationType(
                    scope = eventstore_pb2.ScopeType(
                        type = kwargs.get("scope", eventstore_pb2.Global),
                        bridge = kwargs.get("bridge", None),
                        instance = kwargs.get("instance", None)
                    ),
                    key = key
                )
            )
            resp = self.stub.Get(request)
            return resp
        
def New(server):
    client = Client(server)
    return client
