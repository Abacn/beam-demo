package beam.demo;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class ParseTwitterRecord implements SerializableFunction<GenericRecord, TwitterRecord> {
    @Override
    public TwitterRecord apply(GenericRecord input) {
        Utf8 username = (Utf8) input.get("username");
        Utf8 tweet = (Utf8) input.get("tweet");
        Long timestamp = (Long) input.get("timestamp");

        return new TwitterRecord(username.toString(), tweet.toString(), timestamp);
    }
}
