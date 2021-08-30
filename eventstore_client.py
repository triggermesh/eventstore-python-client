import os
import grpc

import eventstore_pb2
import eventstore_pb2_grpc

class Client:
    def __init__(self, server):
        self.channel = grpc.insecure_channel(server)
        
    def NewKV(self, **kwargs):
        ttl = kwargs.get("ttl", None)
        lockKey = kwargs.get("lockKey", None)
        scope = self.__defineScope(**kwargs)
        return Client.KV(self, scope, ttl, lockKey)
    
    def NewMap(self, key, **kwargs):
        ttl = kwargs.get("ttl", None)
        lockKey = kwargs.get("lockKey", None)
        scope = self.__defineScope(**kwargs)
        return Client.Map(self, key, scope, ttl, lockKey)
    
    def NewQueue(self, key, **kwargs):
        lockKey = kwargs.get("lockKey", None)
        scope = self.__defineScope(**kwargs)
        return Client.Queue(self, key, scope, lockKey)

    def Close(self):
        return self.channel.close()

    def __defineScope(self, **kwargs):
        bridge = kwargs.get("bridge", None)
        instance = kwargs.get("instance", None)
        
        typ = eventstore_pb2.Instance

        if bridge == None and instance == None:
            typ = eventstore_pb2.Global
        elif bridge != None and instance == None:
            typ = eventstore_pb2.Bridge

        return eventstore_pb2.ScopeType(
                    type = typ,
                    bridge = bridge,
                    instance = instance
            )

    class KV(object):
        def __init__(self, client, scope, ttl, lockKey):
            self.ttl = ttl
            self.lockKey = lockKey
            self.scope = scope
            self.stub = eventstore_pb2_grpc.KVStub(client.channel)

        def Set(self, key, value, **kwargs):
            ttl = kwargs.get("ttl", None)
            if ttl == None:
                ttl = self.ttl

            request = eventstore_pb2.SetKVRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
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
                    lockKey = self.lockKey,
                    key = key
                )
            )
            return self.stub.Get(request)
        
        def Del(self, key):
            request = eventstore_pb2.DelKVRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = key
                )
            )
            return self.stub.Del(request)

        def Incr(self, key, incr):
            request = eventstore_pb2.IncrKVRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = key
                )
                incr = incr
            )
            return self.stub.Incr(request)

        def Decr(self, key, decr):
            request = eventstore_pb2.DecrKVRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = key
                )
                decr = decr
            )
            return self.stub.Decr(request)

        def Lock(self, key, lockKey, timeout):
            request = eventstore_pb2.LockRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = lockKey,
                    key = key
                )
                timeout = timeout
            )
            return self.stub.Lock(request)

        def Unlock(self, key, lockKey):
            request = eventstore_pb2.UnlockRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = lockKey,
                    key = key
                )
            )
            return self.stub.Unlock(request)
    
    class Map(object):
        def __init__(self, client, key, scope, ttl, lockKey):
            self.key = key
            self.ttl = ttl
            self.lockKey = lockKey
            self.scope = scope
            self.stub = eventstore_pb2_grpc.MapStub(client.channel)

            self.__new()

        def __new(self):
            request = eventstore_pb2.NewMapRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = self.key
                ),
                ttl = self.ttl
            )
            return self.stub.New(request)

        def SetField(self, field, value):
            request = eventstore_pb2.SetMapFieldRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
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
                    lockKey = self.lockKey,
                    key = self.key
                ),
                field = field
            )
            return self.stub.FieldGet(request)
        
        def GetAll(self):
            request = eventstore_pb2.GetAllMapFieldsRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = self.key
                )
            )
            return self.stub.GetAll(request)

        def Len(self):
            request = eventstore_pb2.LenMapRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = self.key
                )
            )
            return self.stub.Len(request)

        def Del(self):
            request = eventstore_pb2.DelMapRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = self.key
                )
            )
            return self.stub.Del(request)

        def IncrField(self, field, incr):
            request = eventstore_pb2.IncrMapFieldRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = self.key
                ),
                field = field,
                incr = incr
            )
            return self.stub.FieldIncr(request)

        def DecrField(self, field, decr):
            request = eventstore_pb2.DecrMapFieldRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = self.key
                ),
                field = field,
                decr = decr
            )
            return self.stub.FieldDecr(request)

        def DelField(self, field):
            request = eventstore_pb2.DelMapFieldRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = self.key
                ),
                field = field
            )
            return self.stub.FieldDel(request)

        def Lock(self, lockKey, timeout):
            request = eventstore_pb2.LockRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = lockKey,
                    key = self.key
                )
                timeout = timeout
            )
            return self.Stub.Lock(request)

        def Unlock(self, lockKey):
            request = eventstore_pb2.UnlockRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = lockKey,
                    key = self.key
                )
            )
            return self.Stub.Unlock(request)

    class Queue(object):
        def __init__(self, client, key, scope, lockKey):
            self.key = key
            self.scope = scope
            self.lockKey = lockKey
            self.stub = eventstore_pb2_grpc.QueueStub(client.channel)

            self.__new()

        def __new(self):
            request = eventstore_pb2.NewQueueRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = self.key
                )
            )
            return self.stub.New(request)

        def Push(self, value):
            request = eventstore_pb2.PushQueueRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = self.key
                ),
                value = value
            )
            return self.stub.Push(request)

        def Pop(self):
            request = eventstore_pb2.PopQueueRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = self.key
                )
            )
            return self.stub.Pop(request)
        
        def GetAll(self):
            request = eventstore_pb2.GetAllQueueItemsRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = self.key
                )
            )
            return self.stub.GetAll(request)

        def Len(self):
            request = eventstore_pb2.LenQueueRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = self.key
                )
            )
            return self.stub.Len(request)

        def Del(self):
            request = eventstore_pb2.DelQueueRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = self.key
                )
            )
            return self.stub.Del(request)

        def Index(self, index):
            request = eventstore_pb2.IndexQueueRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = self.key
                ),
                index = index
            )
            return self.stub.Index(request)

        def Peek(self):
            request = eventstore_pb2.PeekQueueRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = self.key
                )
            )
            return self.stub.Peek(request)

        def Lock(self, key, timeout):
            request = eventstore_pb2.LockRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = key
                )
                timeout = timeout
            )
            return self.stub.Lock(request)

        def Unlock(self, key):
            request = eventstore_pb2.UnlockRequest(
                location = eventstore_pb2.LocationType(
                    scope = self.scope,
                    lockKey = self.lockKey,
                    key = key
                )
            )
            return self.stub.Unlock(request)

def New(*args):
    server = os.environ.get('EVENTSTORE_URL')
    if len(args) != 0:
        server = args[0]
    return Client(server)