Pipelines, Nodes & Ops
======================

The `Chariots` framework is built around three main types that we use to build a `Chariots` server: Pipelines, Nodes and
Ops. In this article we will go over those three main building blocks in general terms. You can of course check the
:doc:`API documentation<../api_docs/chariots>` to check how to use them technically.

Ops
---

Ops are the atomic computational unit in the `Chariots` framework, meaning that they are part of a more complete
pipeline that couldn't (or at least it wouldn't make sense to) be divided in smaller chunks of instruction. Ops are
actually the only types that are versioned in the framework (also nodes have versions that are derived from ops).

For instance a machine learning model will be an Op versioned according to it's several parameters and it's last
training time.

Also ops have requirements (in terms of number and types that they receive as arguments to their `execute` method) they
are treated as agnostic from the pipeline they are called in (an op can be used in multiple pipelines for instance)

Nodes
-----

Nodes represent a slot in a pipeline meaning. They define the interactions within the pipelines by connecting to their
upstream and downstream node(s). Nodes can be built upon Ops (`Node(my_op)`) but not necessarily, for instance DataNodes
are opless nodes, moreover ABTesting nodes (feature of the upcoming 0.3 release) would nodes using multiple ops. Another
example wound be to use whole pipelines to execute the slot of a node (`Node(my_pipeline`)

For instance a node would represent the preprocessing slot in a pipeline that you could use Op1 or Op2 to fill that slot
(or both in the case of ABTesting).

Pipelines
---------

Pipelines are callable collections of nodes that are exposed to your users through the `Chariots` server (you can also
use them directly but it is not the recommended way of using them). They can also be used inside other pipelines to fill
in a specific node's slot.
