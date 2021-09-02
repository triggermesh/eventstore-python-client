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

"""Module for map type of storage.

Each storage key contains a map with subkeys and values.
Map storage should be used when the scenario needs more than a value per key.
"""
import eventstore_pb2
import eventstore_pb2_grpc

class Map(object):
  """Map class represents the map storage interface with its methods."""
  def __init__(self, client, key, scope, ttl, lock_key):
    self.key = key
    self.ttl = ttl
    self.lock_key = lock_key
    self.scope = scope
    self.stub = eventstore_pb2_grpc.MapStub(client.channel)

    self._new()

  def _new(self):
    request = eventstore_pb2.NewMapRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = self.key
      ),
      ttl = self.ttl
    )
    return self.stub.New(request)

  def set_field(self, field, value):
    request = eventstore_pb2.SetMapFieldRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = self.key
      ),
      field = field,
      value = bytes(value)
    )
    return self.stub.FieldSet(request)

  def get_field(self, field):
    request = eventstore_pb2.GetMapFieldRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = self.key
      ),
      field = field
    )
    return self.stub.FieldGet(request)

  def get_all(self):
    request = eventstore_pb2.GetAllMapFieldsRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = self.key
      )
    )
    return self.stub.GetAll(request)

  def len(self):
    request = eventstore_pb2.LenMapRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = self.key
      )
    )
    return self.stub.Len(request)

  def delete(self):
    request = eventstore_pb2.DelMapRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = self.key
      )
    )
    return self.stub.Del(request)

  def incr_field(self, field, incr):
    request = eventstore_pb2.IncrMapFieldRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = self.key
      ),
      field = field,
      incr = incr
    )
    return self.stub.FieldIncr(request)

  def decr_field(self, field, decr):
    request = eventstore_pb2.DecrMapFieldRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = self.key
      ),
      field = field,
      decr = decr
    )
    return self.stub.FieldDecr(request)

  def del_field(self, field):
    request = eventstore_pb2.DelMapFieldRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = self.lock_key,
        key = self.key
      ),
      field = field
    )
    return self.stub.FieldDel(request)

  def lock(self, lock_key, timeout):
    request = eventstore_pb2.LockRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = lock_key,
        key = self.key
      ),
      timeout = timeout
    )
    return self.Stub.Lock(request)

  def unlock(self, lock_key):
    request = eventstore_pb2.UnlockRequest(
      location = eventstore_pb2.LocationType(
        scope = self.scope,
        lockKey = lock_key,
        key = self.key
      )
    )
    return self.Stub.Unlock(request)
