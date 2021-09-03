import eventstore_client as client

# Create new client instance with the server on provided address.
# If no server address is provided, 
# client will look at EVENTSTORE_URI environment variable.
c = client.new("localhost:8080")

# Create new KV storage with default Global scope and default ttl.
global_kv = c.new_kv()
print("saving first value in the global KV storage")
# Save new key-value pair with individual ttl.
global_kv.set("key1", "value1", ttl=180)
print("retrieving first value from the global KV storage")
print(global_kv.get("key1"))
# Delete key-value pair from the storage.
global_kv.delete("key1")

# Create new KV storage in "GoldenGate" bridge scope and custom ttl value.
bridge_kv = c.new_kv(bridge="GoldenGate", ttl=300)
print("saving second value in the bridge-scoped KV storage")
# Save new key-value pair.
bridge_kv.set("key2", "value2")
print("retrieving second value from the bridge-scoped KV storage")
print(bridge_kv.get("key2"))

# Create new Map storage with default Global scope and default ttl.
global_map = c.new_map("foo-map")
print("setting \"field1\" value")
global_map.set_field("field1", str.encode("value3"))
print("retrieving field value")
print(global_map.get_field("field1"))
# Delete Map storage
global_map.delete()

# Close client channel.
c.close()
