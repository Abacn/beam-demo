# beam-demo
Demonstrates a regression issue when reading gzip-compressed Avro files using `AvroIO.parseFilesGenericRecords`. See below for details on how to replicate

## Apache Beam 2.34.0
The demo works as expected using this version of Apache Beam. To demonstrate:

1. First ensure `def beam_version = "2.34.0"` is set in `build.gradle` (it should be the default on a fresh git clone)
2. ```./gradlew run```

Notice that the code outputs the expected "tweet" contents by reading the zipped Avro files:

```
TWEET: User: migunoTweet: Rock: Nerf paper, scissors is fine.
TWEET: User: migunoTweet: Rock: Nerf paper, scissors is fine.
TWEET: User: BlizzardCSTweet: Works as intended.  Terran is IMBA.
TWEET: User: BlizzardCSTweet: Works as intended.  Terran is IMBA.
```

## Apache Beam 2.45.0
With later versions of Apache Beam, we see unexpected failures with the same code. To demonstrate:

1. First comment out `def beam_version = "2.34.0"` in `build.gradle`
2. Un-comment `def beam_version = "2.45.0"`
3. ```./gradlew run```

Notice that the code fails with the following stack trace:

```
> Task :run FAILED
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
Exception in thread "main" org.apache.beam.sdk.Pipeline$PipelineExecutionException: java.lang.ClassCastException: class java.nio.channels.Channels$ReadableByteChannelImpl cannot be cast to class java.nio.channels.SeekableByteChannel (java.nio.channels.Channels$ReadableByteChannelImpl and java.nio.channels.SeekableByteChannel are in module java.base of loader 'bootstrap')
        at org.apache.beam.runners.direct.DirectRunner$DirectPipelineResult.waitUntilFinish(DirectRunner.java:374)
        at org.apache.beam.runners.direct.DirectRunner$DirectPipelineResult.waitUntilFinish(DirectRunner.java:342)
        at org.apache.beam.runners.direct.DirectRunner.run(DirectRunner.java:218)
        at org.apache.beam.runners.direct.DirectRunner.run(DirectRunner.java:67)
        at org.apache.beam.sdk.Pipeline.run(Pipeline.java:323)
        at org.apache.beam.sdk.Pipeline.run(Pipeline.java:309)
        at beam.demo.Demo.main(Demo.java:52)
Caused by: java.lang.ClassCastException: class java.nio.channels.Channels$ReadableByteChannelImpl cannot be cast to class java.nio.channels.SeekableByteChannel (java.nio.channels.Channels$ReadableByteChannelImpl and java.nio.channels.SeekableByteChannel are in module java.base of loader 'bootstrap')
        at org.apache.beam.sdk.io.AvroSource$AvroReader.startReading(AvroSource.java:743)
        at org.apache.beam.sdk.io.CompressedSource$CompressedReader.startReading(CompressedSource.java:449)
        at org.apache.beam.sdk.io.FileBasedSource$FileBasedReader.startImpl(FileBasedSource.java:479)
        at org.apache.beam.sdk.io.OffsetBasedSource$OffsetBasedReader.start(OffsetBasedSource.java:252)
        at org.apache.beam.sdk.io.ReadAllViaFileBasedSource$ReadFileRangesFn.process(ReadAllViaFileBasedSource.java:140)
```

## Compressed vs Un-compressed Avro Files
One important point worth noting is that this issue only seems to occur for compressed files. If we read in the un-compressed versions then the code works as expected. To demonstrate:
1. Update `Demo.java` so that `LinkedList<String> files` reads in `"twitter1.avro"` and `"twitter1.avro"`, instead of the compressed versions
2. ```./gradlew run```