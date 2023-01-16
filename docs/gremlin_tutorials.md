# Tutorial for Gremlin Users

This tutorial provides instructions for Gremlin users to get your hands on GraphScope,
or more specifically the GIE engine. The tutorial assumes that the engine has been
properly deployed (see the documentations for various deployments).

GraphScope provides two ways of interacting with the GIE engine depending on how
the system is deployed. In the default deployment, GIE can be accessed
in the python client with the other components of GraphScope, which is detailed
in [Python Tutorial](./python_tutorials.md).
In the deployment with Groot (see [Deploy With Groot](./deploy_as_service_with_groot.md)), 
GIE is a standalone service that can provide interactive querying service just like
starting a native [Gremlin server](https://tinkerpop.apache.org/docs/current/reference/#connecting-gremlin-server).
In this case, the GIE service is exposed as an endpoint like the official Gremlin
service, to which all official methods of connecting and interacting with the service
can be used, including the [Gremlin console](https://tinkerpop.apache.org/docs/current/reference/#gremlin-console),
and sdks of a variety of programming languages. 

## Connecting from Gremlin Console
In the case of deployment with Groot, users can connect to the GIE service
from the Gremlin console, which is similar to the [official process](https://tinkerpop.apache.org/docs/current/reference/#connecting-via-console).
In case that the information of endpoint of the Gremlin service remain unknown to this point, 
it can be obtained using the k8s command as:
```bash
todo
```

After getting the endpoint, created the file of `remote.yaml` that writes:
```yaml 
todo
```

Then in the Gremlin console, types:
```bash
gremlin> :remote connect tinkerpop.server /path/to/remote.yaml
gremlin> :remote console
```

