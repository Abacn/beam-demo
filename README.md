# beam-demo

Demostrate a BigQueryIO direct read issue where not all data are read.

run on dataflow runner: 
```
./gradlew run --args="--runner=DataflowRunner --project=*** --region=us-central1 --tempLocation=*** --enableBundling=True"
```