import client

c = client.New("localhost:8080")
kv = c.KV()
print(kv.Set("foo", str.encode("bar")))
# print(kv.Get("foo"))

c.Close()