# Building Blocks of Datapipe

The subject of description in `Datapipe` is a data transformation graph, which we call a `Pipeline`.

`Datapipe` models a pipeline as a sequence of `Table`s connected by `Transform`s: each transform reads from one or more input tables and writes its results to one or more output tables.
