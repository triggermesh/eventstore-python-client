import eventstore_pb2
import eventstore_pb2_grpc

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
            ),
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