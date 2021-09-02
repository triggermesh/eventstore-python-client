# Eventstore Python client

Eventstore Python client provides a way to work with Eventstore service from Python code. Classes and methods available in this client are implementing Eventstore interface described in the [specification](https://github.com/triggermesh/eventstore#storage-service-interface).


### Usage

Eventstore client is meant to run inside the containers in k8s cluster. For debugging purposes, it is possible to run client locally but an instance of Evenstore is still required to be accessible over the network. Follow the instructions below to setup and run sample Python code that works with Eventstore service. 

### Prerequisites

1. Create Eventstore storage:

```
cat <<EOF | kubectl apply -f -
apiVersion: eventstores.triggermesh.io/v1alpha1
kind: InMemoryStore
metadata:
  name: inmemory-test
EOF
```

2. Forward Evenstore service port to a local machine:

```
kubectl port-forward svc/inmemorystorage-inmemory-test 8080
```

#### Client usage 

1. In the new terminal clone the repository:

```
git clone git@github.com:triggermesh/eventstore-python-client.git
```

2. Make the module importable. The simplest way to do that would be setting `PYTHONPATH` env variable:
  
```
export PYTHONPATH=$(pwd)/eventstore-python-client/eventstore
```

3. Run sample code:

```
python eventstore-python-client/sample.py
```

If previous steps were completed without the errors, the client should connect to Eventstore service, create KV and Map storage, set and retrieve test values. Python code in [sample.py](./sample.py) demonstrates how simple Eventstore API and the client are.

## Support

We would love your feedback and help on this project, so don't hesitate to let us know what is wrong and how we could improve them, just file an [issue](https://github.com/triggermesh/eventstore-python-client/issues/new) or join those of us who are maintaining them and submit a PR.

## Commercial Support

TriggerMesh Inc supports this project commercially, email info@triggermesh.com to get more details.

## Code of Conduct

This project is by no means part of [CNCF](https://www.cncf.io/) but we abide
by its
[code of conduct](https://github.com/cncf/foundation/blob/master/code-of-conduct.md)
