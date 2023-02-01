# GraphScope for Graph Interactive Queries

GraphScope Interactive Engine (GIE) is a distributed system designed specifically to make it easy
for a variety of users to analyze large and complex graph structures in an iterative manner.
It provides high-level language for interactive graph queries, and provides automatic parallel
execution. Currently, GIE has supported [Tinkerpop](https://tinkerpop.apache.org)'s Gremlin,
both its imperative traversal and declarative pattern matching.

## Apache TinkerPop
Apache [TinkerPop](https://tinkerpop.apache.org) is an open framework for developing interactive
graph applications using the Gremlin query language. GIE implements TinkerPop’s Gremlin Server
interface so that the system can seamlessly interact with the TinkerPop ecosystem, including
the language wrappers of Python and Gremlin's console. We recommend connecting with GIE in
Python.

## Connecting within Python
GraphScope makes it easy to connect to a loaded graph within Python as shown below:

```Python
import graphscope
from graphscope.dataset import load_ldbc

# create a new session, load LDBC graph (as an example),
# and get the Gremlin entry point
sess = graphscope.session(num_workers=2)
graph = load_ldbc(sess, prefix='/path/to/ldbc_sample')
interactive = sess.gremlin(graph)

# check the total node_num and edge_num using Gremlin
node_num = interactive.execute('g.V().count()').one()
edge_num = interactive.execute("g.E().count()").one()
```

In fact, the `interactive` object is an instance of the `InteractiveQuery` Python class, which is a
simple wrapper around [Gremlin-Python](https://pypi.org/project/gremlinpython/) that implements
Gremlin using the Python language.

Each graph loaded in GraphScope is associated with a Gremlin endpoint (or URL) for a client to
connect remotely to, which can be obtained as follows:
```Python
print(interactive.graph_url)
```

You should see the following output:
```bash
ws://your-endpoint:your-ip/gremlin
```

With that information, we can directly query a graph using Gremlin-Python as described at
[here](https://tinkerpop.apache.org/docs/current/reference/#gremlin-python).

## Programming with Gremlin
GIE is designed to faithfully preserve the programming model of Gremlin, and as a result it can
be used to scale existing Gremlin applications to large compute clusters with minimum modification.
In this section, we provide a high-level view of the programming model, highlighting the key concepts
including the data model and query language.
See [here](./supported_gremlin_steps.md) for a complete supported list of Gremlin.

### Data model
Gremlin enables users to define ad-hoc traversals on property graphs. A property graph is a directed
graph in which vertices and edges can have a set of properties. Every entity (vertex or edge) is
identified by a unique identifier (`ID`), and has a (`label`) indicating its type or role.
Each property is a key-value pair with combination of entity `ID` and property name as the key.

<center>
   <img style="border-radius: 0.3125em;
   box-shadow: 0 2px 4px 0 rgba(34,36,38,.12),0 2px 10px 0 rgba(34,36,38,.08);"
   src="./images/property_graph.png" width="60%">
   <br>
   <div style="color:orange; border-bottom: 1px solid #d9d9d9;
   display: inline-block;
   color: #999;
   padding: 2px;">An example of property graph.</div>
</center>

The above figure shows an example property graph. It contains `user`, `product`, and `address` vertices
connected by `order`, `deliver`, `belongs_to`, and `home_of` edges. A path following vertices `1–>2–>3`,
shown as the dotted line, indicates that a buyer "Tom" ordered a product "gift" offered by a seller
"Jack", with a price of "$99".

### Query language
In a Gremlin traversal, a set of traversers walk a graph according to particular user-provided instructions,
and the result of the traversal is the collection of all halted traversers. A traverser is the basic
unit of data processed by a Gremlin engine. Each traverser maintains a location that is a reference
to the current vertex, edge or property being visited, and (optionally) the path history with application state.

The flexibility of Gremlin mainly stems from nested traversal, which allows a traversal to be embedded
within another operator, and used as a function to be invoked by the enclosing operator for processing input.
The role and signature of the function are determined by the type of the enclosing operator.

For example, a nested traversal within the `where` operator acts as a predicate function for
conditional filters, while that within the `select` or `order` operator maps each traverser
to the output or ordering key for sorting the output, respectively.

Below shows a Gremlin query for cycle detection, which tries to find cyclic paths of length
`k` starting from a given account.

```groovy
g.V('account').has('id','2').as('s')
 .out('k-1..k', 'transfer')
 .with('PATH_OPT', 'SIMPLE')
 .endV()
 .where(out('transfer').eq('s'))
 .limit(1)
```

First, the source operator `V` (with the has filter) returns all the account vertices with an
identifier of `2`. The as operator is a modulator that does not change the input collection of
traversers but introduces a name (`s` in this case) for later references. Second, it traverses
the outgoing transfer edges for exact `k-1` times ( `out()` with a range of lower bound `k-1` (included)
and upper bound `k` (excluded)), skipping any repeated vertices (`with()` the `SIMPLE` path option).
Such a multi-hop [path expansion](./supported_gremlin_steps.md) is a syntactic sugar we
introduce for easily handling the path-related applications.
Third, the `where` operator checks if the starting vertex s can be reached by one more step, that is,
whether a cycle of length k is formed. Finally, the `limit` operator at the end indicates
only one such result is needed.

## Compatibility with TinkerPop
GIE supports the property graph model and Gremlin traversal language defined by Apache TinkerPop,
and provides a Gremlin Websockets server that supports TinkerPop version 3.3 and 3.4.
In addition to the original Gremlin queries, we further introduce some syntactic sugars to allow
more succinct expression. In this section, we provide an overview of the key differences
between our implementation of Gremlin and the Apache TinkerPop specification.

### Property graph constraints
The current release (MaxGraph) leverages Vineyard to supply an in-memory store for immutable
graph data that can be partitioned across multiple servers. By design, it introduces the following constraints:

 - Each graph has a schema comprised of the edge labels, property keys, and vertex labels used therein.
 - Each vertex type or label has a primary key (property) defined by user. The system will automatically
  generate a String-typed unique identifier for each vertex and edge, encoding both the label information
  as well as user-defined primary keys (for vertex).
 - Each vertex or edge property can be of the following data types: `int`, `long`, `float`, `double`,
  `String`, `List<int>`, `List<long>`, and `List<String>`.

### Limitations
Because of the distributed nature and practical considerations, a few features are not supported:
- Functionalities
  -- Graph mutations.
  -- Lambda and Groovy expressions and functions, such as the `.map{<expression>}`, the
  `.by{<expression>}`, and the `.filter{<expression>}` functions, 1+1, and
  `System.currentTimeMillis()`, etc. We have provided the `expr()` syntactic sugar to handle
  expressions.
  -- Gremlin traversal strategies.
  -- Transactions.
  -- Secondary index isn’t currently available. Primary keys will be automatically indexed.

- Gremlin Steps: See [here](./supported_gremlin_steps.md) for a complete supported/unsupported list of Gremlin.



