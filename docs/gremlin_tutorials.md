# Tutorial for Gremlin Users

This tutorial provides instructions for Gremlin users to get your hands on GraphScope,
or more specifically the GIE engine. The tutorial assumes that the engine has been
properly deployed (see the documentations for various deployments).

GraphScope provides two ways of interacting with the GIE engine depending on how
the system is deployed. In the default deployment, GIE can be accessed
in the python client with the other components of GraphScope, which is detailed
in [Python Tutorial](./python_tutorials.md).
In the [deployment with Groot](./deploy_as_service_with_groot.md)),
GIE is a standalone service that can provide interactive querying service just like
starting a native [Gremlin server](https://tinkerpop.apache.org/docs/current/reference/#connecting-gremlin-server).
In this case, the GIE service is exposed as an endpoint like the official Gremlin
service, to which all official methods of connecting and interacting with the service
can be used, including the [Gremlin console](https://tinkerpop.apache.org/docs/current/reference/#gremlin-console),
and sdks of a variety of programming languages.

## Connecting from Gremlin Console
In the deployment with Groot, you can connect to the GIE service
from the Gremlin console just as specified in the [Tinkerpop documentation](https://tinkerpop.apache.org/docs/current/reference/#connecting-via-console).
The endpoint of the Gremlin service, if unknown, can be obtained using the k8s command as:
```bash
# get external ip and port of frontend service from k8s
kubectl get service -n graphscope-store-frontend

NAME         TYPE           CLUSTER-IP     EXTERNAL-IP      PORT(S)    AGE
my-service   LoadBalancer xxx.xxx.xxx.xxx yyy.yyy.yyy.yyy   pppp/TCP   54s
```

After getting the endpoint, you can manually create the file of `remote.yaml` that writes:
```yaml
# config EXTERNAL-IP given above
hosts: [yyy.yyy.yyy.yyy]
# config PORT given above
port: pppp
serializer: { className: org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1, config: { serializeResultToString: true }}
```

Then in the Gremlin console, types:
```bash
gremlin> :remote connect tinkerpop.server /path/to/remote.yaml
gremlin> :remote console
gremlin> g.V().count()  # querying the graph given in the deployment,
```

## Querying with Gremlin
GIE attempts to maintain the official syntax of Gremlin, in order to make the use of GIE similar to that of Tinkerpop. GIE has supported a lot of widely-used [Gremlin steps](./supported_gremlin_steps.md). Due to a variety of reasons, we've decided not including certain steps in Gremlin, including `repeat()` (most cases can be implemented via the syntactic sugar of path expand), `properties()` (similar to `valueMap()`), `branch()` and `choose()`, and side effects such as `sack()`, `aggregate()` (global variables are difficult to maintain in the distributed context). Please refer to the [documentation](./supported_gremlin_steps.md) for the details of the supported Gremlin steps and limitations in GIE.

In this tutorial, we will only showcase the different use cases in GIE, compared to Tinkerpop.
Here, we assume users are querying through the Gremlin console, which implies that GIE is deployed in a standalone mode with groot to maintain the graph data. After the deployment, the graph must have been created and loaded into groot, and thus we do not need to write `graph = TinkerFactory.createModern()` as in Tinkerpop to specify the graph data. The graph traversal `g` is also implicitly created and ready to use as long as the console is connected with the remote server (after completing the command of  `:remote console`).
We still use the [modern graph](https://tinkerpop.apache.org/docs/current/tutorials/getting-started/) to demonstrate the running examples.

