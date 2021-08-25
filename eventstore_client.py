import grpc

import eventstore_pb2
import eventstore_pb2_grpc

class Client:
    def __init__(self, server):
        self.channel = grpc.insecure_channel(server)
        
    def NewKV(self, **kwargs):
        ttl = kwargs.get("ttl", None)
        scope = self.__defineScope(**kwargs)
        return Client.KV(self, scope, ttl)
    
    def NewMap(self, key, **kwargs):
        ttl = kwargs.get("ttl", None)
        scope = self.__defineScope(**kwargs)
        return Client.Map(self, key, scope, ttl)
    
    def NewQueue(self, **kwargs):
        scope = self.__defineScope(**kwargs)
        return Client.Queue(self, scope)

    def Close(self):
        return self.channel.close()

    def __defineScope(self, **kwargs):
        bridge = kwargs.get("bridge", None)
        instance = kwargs.get("instance", None)

        if bridge == None and instance == None:
            typ = eventstore_pb2.Global
        elif bridge != None and instance == None:
            typ = eventstore_pb2.Bridge
        else:
            typ = eventstore_pb2.Instance

        return eventstore_pb2.ScopeType(
                    type = typ,
                    bridge = bridge,
                    instance = instance
            )

    class KV(object):
        def __init__(self, client, scope, ttl):
            self.ttl = ttl
            self.scope = scope
            self.stub = eventstore_pb2_grpc.KVStub(client.channel)

        def Set(self, key, value, **kwargs):
            ttl = kwargs.get("ttl", None)
            if ttl == None:
                ttl = self.ttl

            request = eventstore_pb2.SetKVRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    key = key
                ),
                ttl = ttl,
                value = value
            )
            return self.stub.Set(request)

        def Get(self, key):
            request = eventstore_pb2.GetKVRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    key = key
                )
            )
            return self.stub.Get(request)
        
        def Del(self, key):
            request = eventstore_pb2.DelKVRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    key = key
                )
            )
            return self.stub.Del(request)

        def Incr(self):
            pass
        def Decr(self):
            pass
        def Lock(self):
            pass
        def Unlock(self):
            pass
    
    class Map(object):
        def __init__(self, client, key, scope, ttl):
            self.key = key
            self.ttl = ttl
            self.scope = scope
            self.stub = eventstore_pb2_grpc.MapStub(client.channel)

            self.__new()

        def __new(self):
            request = eventstore_pb2.NewMapRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    key = self.key
                ),
                ttl = self.ttl
            )
            return self.stub.New(request)

        def SetField(self, field, value):
            request = eventstore_pb2.SetMapFieldRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    key = self.key
                ),
                field = field,
                value = value
            )
            return self.stub.FieldSet(request)

        def GetField(self, field):
            request = eventstore_pb2.GetMapFieldRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    key = self.key
                ),
                field = field
            )
            return self.stub.FieldGet(request)
        
        def GetFields(self):
            request = eventstore_pb2.GetAllMapFieldsRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    key = self.key
                )
            )
            return self.stub.GetFields(request)

        def Len(self):
            request = eventstore_pb2.LenMapRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    key = self.key
                )
            )
            return self.stub.Len(request)

        def Del(self):
            request = eventstore_pb2.DelMapRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    key = self.key
                )
            )
            return self.stub.Del(request)

        def IncrField():
            pass
        def DecrField():
            pass
        def DelField():
            pass
        def Lock():
            pass
        def Unlock():
            pass

    # Not implemented yet
    class Queue(object):
        def __init__(self, client, scope):
            self.scope = scope
            self.stub = eventstore_pb2_grpc.QueueStub(client.channel)

        def New(self, key, **kwargs):
            request = eventstore_pb2.NewQueueRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    key = key
                ),
                ttl = kwargs.get("ttl", None),
            )

        def Push(self, key, value, **kwargs):
            request = eventstore_pb2.PushQueueRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    key = key
                ),
                value = value
            )
            resp = self.stub.Set(request)
            return resp

        def Pop(self, key, **kwargs):
            request = eventstore_pb2.PopQueueRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    key = key
                )
            )
            resp = self.stub.Get(request)
            return resp
        
        def GetAll(self):
            pass
        def Len(self):
            pass
        def Del(self):
            pass
        def Index(self):
            pass
        def Peek(self):
            pass

def Connect(server):
    return Client(server)
