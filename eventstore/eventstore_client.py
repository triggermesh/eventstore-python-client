import os
import grpc

import eventstore_pb2

from eventstore_kv import KV
from eventstore_map import Map
from eventstore_queue import Queue

class Client:
  def __init__(self, server):
    self.channel = grpc.insecure_channel(server)

  def NewKV(self, **kwargs):
    ttl = kwargs.get("ttl", None)
    lock_key = kwargs.get("lock_key", None)
    scope = self.__define_scope(**kwargs)
    return KV(self, scope, ttl, lock_key)

  def NewMap(self, key, **kwargs):
    ttl = kwargs.get("ttl", None)
    lock_key = kwargs.get("lock_key", None)
    scope = self.__define_scope(**kwargs)
    return Map(self, key, scope, ttl, lock_key)

  def NewQueue(self, key, **kwargs):
    lock_key = kwargs.get("lock_key", None)
    scope = self.__define_scope(**kwargs)
    return Queue(self, key, scope, lock_key)

  def Close(self):
    return self.channel.close()

  def __define_scope(self, **kwargs):
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

def New(*args):
  server = os.environ.get("EVENTSTORE_URL")
  if len(args) != 0:
    server = args[0]
  return Client(server)
