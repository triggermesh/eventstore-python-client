import eventstore_pb2
import eventstore_pb2_grpc

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
            ),
            incr = incr
        )
        return self.stub.Incr(request)

    def Decr(self, key, decr):
        request = eventstore_pb2.DecrKVRequest(
            location = eventstore_pb2.LocationType(
                scope = self.scope,
                lockKey = self.lockKey,
                key = key
            ),
            decr = decr
        )
        return self.stub.Decr(request)

    def Lock(self, key, lockKey, timeout):
        request = eventstore_pb2.LockRequest(
            location = eventstore_pb2.LocationType(
                scope = self.scope,
                lockKey = lockKey,
                key = key
            ),
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