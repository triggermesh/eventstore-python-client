# Copyright (c) 2021 TriggerMesh Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Module for queue type of storage.

Queue: each storage key contains a FIFO queue.
Queue storage should be used when items needs to be processed in order.
"""
import eventstore_pb2
import eventstore_pb2_grpc

class Queue(object):
  """Queue class represents the queue storage interface with its methods."""
  def __init__(self, client, key, scope, lock_key):
    self.key = key
    self.scope = scope
    self.lock_key = lock_key
    self.stub = eventstore_pb2_grpc.QueueStub(client.channel)

    self._new()

  def _new(self):
    request = eventstore_pb2.NewQueueRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = self.key
      )
    )
    return self.stub.New(request)

  def push(self, value):
    request = eventstore_pb2.PushQueueRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = self.key
      ),
      value = bytes(value)
    )
    return self.stub.Push(request)

  def pop(self):
    request = eventstore_pb2.PopQueueRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = self.key
      )
    )
    return self.stub.Pop(request)

  def get_all(self):
    request = eventstore_pb2.GetAllQueueItemsRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = self.key
      )
    )
    return self.stub.GetAll(request)

  def len(self):
    request = eventstore_pb2.LenQueueRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = self.key
      )
    )
    return self.stub.Len(request)

  def delete(self):
    request = eventstore_pb2.DelQueueRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = self.key
      )
    )
    return self.stub.Del(request)

  def index(self, index):
    request = eventstore_pb2.IndexQueueRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = self.key
      ),
      index = index
    )
    return self.stub.Index(request)

  def peek(self):
    request = eventstore_pb2.PeekQueueRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = self.key
      )
    )
    return self.stub.Peek(request)

  def lock(self, key, timeout):
    request = eventstore_pb2.LockRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = key
      ),
      timeout = timeout
    )
    return self.stub.Lock(request)

  def unlock(self, key):
    request = eventstore_pb2.UnlockRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = key
      )
    )
    return self.stub.Unlock(request)
