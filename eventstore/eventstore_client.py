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

"""TriggerMesh Eventstore client.

EventStore is an interface for storing ephemeral data in an event flow.

Usage:

c = client.new("localhost:8080")
kv = c.new_kv()
kv.set("foo", str.encode("bar"), ttl=180)
print(kv.get("foo"))
"""

import os
import grpc

import eventstore_pb2

from eventstore_kv import KV
from eventstore_map import Map
from eventstore_queue import Queue

class Client:
  """Client is a parent class of this module."""
  def __init__(self, server):
    self.channel = grpc.insecure_channel(server)

  def new_kv(self, **kwargs):
    ttl = kwargs.get("ttl", None)
    lock_key = kwargs.get("lock_key", None)
    scope = self._define_scope(**kwargs)
    return KV(self, scope, ttl, lock_key)

  def new_map(self, key, **kwargs):
    ttl = kwargs.get("ttl", None)
    lock_key = kwargs.get("lock_key", None)
    scope = self._define_scope(**kwargs)
    return Map(self, key, scope, ttl, lock_key)

  def new_queue(self, key, **kwargs):
    lock_key = kwargs.get("lock_key", None)
    scope = self._define_scope(**kwargs)
    return Queue(self, key, scope, lock_key)

  def close(self):
    return self.channel.close()

  def _define_scope(self, **kwargs):
    bridge = kwargs.get("bridge", None)
    instance = kwargs.get("instance", None)

    typ = eventstore_pb2.Instance
    if bridge is None and instance is None:
      typ = eventstore_pb2.Global
    elif bridge is not None and instance is None:
      typ = eventstore_pb2.Bridge
    return eventstore_pb2.ScopeType(
            type = typ,
            bridge = bridge,
            instance = instance
      )

def new(*args):
  server = os.environ.get("EVENTSTORE_URL")
  if len(args) != 0:
    server = args[0]
  return Client(server)
