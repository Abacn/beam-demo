package beam.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class Demo {

    public static void main(String[] args) {
        File resourcesDirectory = new File("src/main/resources");

        /*
          Un-comment the below to demonstrate that un-compressed files
          with the same contents can be read on both versions of Beam
         */

//        LinkedList<String> files = new LinkedList<String>(List.of(
//                "twitter1.avro",
//                "twitter2.avro"
//        ));

        LinkedList<String> files = new LinkedList<String>(List.of(
                "twitter1.avro.gz",
                "twitter2.avro.gz"
        ));

        List<String> fileNames = files.stream()
                .map(f -> "file://" + resourcesDirectory.getAbsolutePath() + "/" + f)
                .collect(Collectors.toList());

        PipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation().create();

        Pipeline p = Pipeline.create(options);
        p
                .apply("File Names", Create.of(fileNames))
                .apply("Match", FileIO.matchAll())
                .apply("Read", FileIO.readMatches())
                .apply("AvroDecode", AvroIO.parseFilesGenericRecords(new ParseTwitterRecord())
                        .withCoder(TwitterRecord.coder()
                        ))
                .apply(ParDo.of(new DoFn<TwitterRecord, TwitterRecord>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println("TWEET: User: " + c.element().getUsername() + ". Tweet: " + c.element().getTweet());
                    }
                }));
        p.run();
    }
}
