# beam-demo

Demostrate a BigQueryIO direct read issue where not all data are read (https://github.com/apache/beam/issues/26354).

run on dataflow runner: 
```
./gradlew run --args="--runner=DataflowRunner --project=*** --region=us-central1 --tempLocation=*** --enableBundling=True"
```