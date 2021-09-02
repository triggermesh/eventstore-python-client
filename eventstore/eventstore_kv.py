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

"""Module for key-value type of storage."""
import eventstore_pb2
import eventstore_pb2_grpc

class KV(object):
  """KV class represents the key-value storage interface with its methods."""
  def __init__(self, client, scope, ttl, lock_key):
    self.ttl = ttl
    self.lock_key = lock_key
    self.scope = scope
    self.stub = eventstore_pb2_grpc.KVStub(client.channel)

  def set(self, key, value, **kwargs):
    ttl = kwargs.get("ttl", None)
    if ttl is None:
      ttl = self.ttl
    request = eventstore_pb2.SetKVRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = key
      ),
      ttl = ttl,
      value = bytes(value, "utf-8")
    )
    return self.stub.Set(request)

  def get(self, key):
    request = eventstore_pb2.GetKVRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = key
      )
    )
    return self.stub.Get(request)

  def delete(self, key):
    request = eventstore_pb2.DelKVRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = key
      )
    )
    return self.stub.Del(request)

  def incr(self, key, incr):
    request = eventstore_pb2.IncrKVRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = key
      ),
      incr = incr
    )
    return self.stub.Incr(request)

  def decr(self, key, decr):
    request = eventstore_pb2.DecrKVRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = key
      ),
      decr = decr
    )
    return self.stub.Decr(request)

  def lock(self, key, lock_key, timeout):
    request = eventstore_pb2.LockRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = lock_key,
        key = key
      ),
      timeout = timeout
    )
    return self.stub.Lock(request)

  def unlock(self, key, lock_key):
    request = eventstore_pb2.UnlockRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = lock_key,
        key = key
      )
    )
    return self.stub.Unlock(request)
