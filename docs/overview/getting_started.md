# Getting Started

This tutorial will give you a quick tour of GraphScope's features. 
To get started, we’ll start by installing GraphScope. Most of the examples in this guide are based on Python on your local machine, 
but we will also show you how to deploy and run GraphScope on a k8s cluster.

```bash
pip3 install graphscope
```

## One-stop Graph Processing

We will use a walking-through example to show you how to use GraphScope to 
process various graph computation tasks in a one-stop manner.

The example is targeting node classification on a citation network.

ogbn-mag is a heterogeneous network composed of a subset of the Microsoft Academic Graph. It contains 4 types of entities(i.e., papers, authors, institutions, and fields of study), as well as four types of directed relations connecting two entities.

Given the heterogeneous ogbn-mag data, the task is to predict the class of each paper. Node classification can identify papers in multiple venues, which represent different groups of scientific work on different topics. We apply both the attribute and structural information to classify papers. In the graph, each paper node contains a 128-dimensional word2vec vector representing its content, which is obtained by averaging the embeddings of words in its title and abstract. The embeddings of individual words are pre-trained. The structural information is computed on-the-fly.

GraphScope models graph data as property graph, in which the edges/vertices are labeled and have many properties. Taking ogbn-mag as example, the figure below shows the model of the property graph.

:::{figure-md}

<img src="../images/sample_pg.png"
     alt="Sample of property graph."
     width="80%">

Sample of property graph 
:::

This graph has four kinds of vertices, labeled as paper, author, institution and field_of_study. There are four kinds of edges connecting them, each kind of edges has a label and specifies the vertex labels for its two ends. For example, cites edges connect two vertices labeled paper. Another example is writes, it requires the source vertex is labeled author and the destination is a paper vertex. All the vertices and edges may have properties. e.g., paper vertices have properties like features, publish year, subject label, etc.

````{dropdown} Import GraphScope and load a graph

To load this graph to GraphScope with our retrieval module, please use these code.

```python
import graphscope
from graphscope.dataset import load_ogbn_mag

g = load_ogbn_mag()
```
````

Interactive queries allow users to directly explore, examine, and present graph data in an exploratory manner in order to locate specific or in-depth information in time. GraphScope adopts a high-level language called Gremlin for graph traversal, and provides efficient execution at scale.


````{dropdown} Run interactive queries with Gremlin

In this example, we use graph traversal to count the number of papers two given authors have co-authored. To simplify the query, we assume the authors can be uniquely identified by ID 2 and 4307, respectively.


```python
# get the endpoint for submitting Gremlin queries on graph g.
interactive = graphscope.gremlin(g)

# count the number of papers two authors (with id 2 and 4307) have co-authored
papers = interactive.execute("g.V().has('author', 'id', 2).out('writes').where(__.in('writes').has('id', 4307)).count()").one()
```
````

Graph analytics is widely used in real world. Many algorithms, like community detection, paths and connectivity, centrality are proven to be very useful in various businesses. GraphScope ships with a set of built-in algorithms, enables users easily analysis their graph data.


````{dropdown} Run analytical algorithms over graph

Continuing our example, below we first derive a subgraph by extracting publications in specific time out of the entire graph (using Gremlin!), and then run k-core decomposition and triangle counting to generate the structural features of each paper node.

Please note that many algorithms may only work on homogeneous graphs, and therefore, to evaluate these algorithms over a property graph, we need to project it into a simple graph at first.

```python
# extract a subgraph of publication within a time range
sub_graph = interactive.subgraph("g.V().has('year', gte(2014).and(lte(2020))).outE('cites')")

# project the projected graph to simple graph.
simple_g = sub_graph.project(vertices={"paper": []}, edges={"cites": []})

ret1 = graphscope.k_core(simple_g, k=5)
ret2 = graphscope.triangles(simple_g)

# add the results as new columns to the citation graph
sub_graph = sub_graph.add_column(ret1, {"kcore": "r"})
sub_graph = sub_graph.add_column(ret2, {"tc": "r"})
```
````

Graph neural networks (GNNs) combines superiority of both graph analytics and machine learning. GNN algorithms can compress both structural and attribute information in a graph into low-dimensional embedding vectors on each node. These embeddings can be further fed into downstream machine learning tasks.


````{dropdown} Prepare data and engine for learning


In our example, we train a GCN model to classify the nodes (papers) into 349 categories, each of which represents a venue (e.g. pre-print and conference). To achieve this, first we launch a learning engine and build a graph with features following the last step.

```python
# define the features for learning
paper_features = []
for i in range(128):
    paper_features.append("feat_" + str(i))

paper_features.append("kcore")
paper_features.append("tc")

# launch a learning engine.
lg = graphscope.graphlearn(sub_graph, nodes=[("paper", paper_features)],
                  edges=[("paper", "cites", "paper")],
                  gen_labels=[
                      ("train", "paper", 100, (0, 75)),
                      ("val", "paper", 100, (75, 85)),
                      ("test", "paper", 100, (85, 100))
                  ])
```
````
Then we define the training process, and run it.


````{dropdown} Define the training process and run it

```python
try:
    # https://www.tensorflow.org/guide/migrate
    import tensorflow.compat.v1 as tf
    tf.disable_v2_behavior()
except ImportError:
    import tensorflow as tf

import graphscope.learning
from graphscope.learning.examples import EgoGraphSAGE
from graphscope.learning.examples import EgoSAGESupervisedDataLoader
from graphscope.learning.examples.tf.trainer import LocalTrainer

# supervised GCN.
def train_gcn(graph, node_type, edge_type, class_num, features_num,
              hops_num=2, nbrs_num=[25, 10], epochs=2,
              hidden_dim=256, in_drop_rate=0.5, learning_rate=0.01,
):
    graphscope.learning.reset_default_tf_graph()

    dimensions = [features_num] + [hidden_dim] * (hops_num - 1) + [class_num]
    model = EgoGraphSAGE(dimensions, act_func=tf.nn.relu, dropout=in_drop_rate)

    # prepare train dataset
    train_data = EgoSAGESupervisedDataLoader(
        graph, graphscope.learning.Mask.TRAIN,
        node_type=node_type, edge_type=edge_type, nbrs_num=nbrs_num, hops_num=hops_num,
    )
    train_embedding = model.forward(train_data.src_ego)
    train_labels = train_data.src_ego.src.labels
    loss = tf.reduce_mean(
        tf.nn.sparse_softmax_cross_entropy_with_logits(
            labels=train_labels, logits=train_embedding,
        )
    )
    optimizer = tf.train.AdamOptimizer(learning_rate=learning_rate)

    # prepare test dataset
    test_data = EgoSAGESupervisedDataLoader(
        graph, graphscope.learning.Mask.TEST,
        node_type=node_type, edge_type=edge_type, nbrs_num=nbrs_num, hops_num=hops_num,
    )
    test_embedding = model.forward(test_data.src_ego)
    test_labels = test_data.src_ego.src.labels
    test_indices = tf.math.argmax(test_embedding, 1, output_type=tf.int32)
    test_acc = tf.div(
        tf.reduce_sum(tf.cast(tf.math.equal(test_indices, test_labels), tf.float32)),
        tf.cast(tf.shape(test_labels)[0], tf.float32),
    )

    # train and test
    trainer = LocalTrainer()
    trainer.train(train_data.iterator, loss, optimizer, epochs=epochs)
    trainer.test(test_data.iterator, test_acc)

train_gcn(lg, node_type="paper", edge_type="cites",
          class_num=349,  # output dimension
          features_num=130,  # input dimension, 128 + kcore + triangle count
)
```
````


## Graph Interactive Query Quick Start

TBF

## Graph Analytics Quick Start

TBF

## Graph Learning Quick Start

TBF

