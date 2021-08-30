import eventstore_pb2
import eventstore_pb2_grpc

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
            ),
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