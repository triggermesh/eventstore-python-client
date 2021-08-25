import eventstore_client as client

c = client.Connect("localhost:8080")

globalKV = c.NewKV()
globalKV.Set("foo", str.encode("global"), ttl=180)
# print(globalKV.Del("foo"))
print(globalKV.Get("foo"))

bridgeKV = c.NewKV(bridge="bridge-foo", ttl=300)
bridgeKV.Set("foo", str.encode("bridge"))
# print(bridgeKV.Del("foo"))
print(bridgeKV.Get("foo"))

# globalMap = c.NewMap("foo-map")
# globalMap.SetField("i", str.encode("j"))
# # globalMap.Del()
# print(globalMap.GetField("i"))

c.Close()
