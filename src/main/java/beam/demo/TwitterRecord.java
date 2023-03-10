package beam.demo;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.util.VarInt;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

public class TwitterRecord {
    private final static TwitterRecordCoder CODER = new TwitterRecordCoder();
    private final String username;
    private final String tweet;
    private final Long timestamp;

    public TwitterRecord(String username, String tweet, Long timestamp) {
        this.username = username;
        this.tweet = tweet;
        this.timestamp = timestamp;
    }

    public static TwitterRecordCoder coder() {
        return CODER;
    }

    public String getUsername() {
        return username;
    }

    public String getTweet() {
        return tweet;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public static class TwitterRecordCoder extends Coder<TwitterRecord> {
        private TwitterRecordCoder() {

        }

        @Override
        public void encode(TwitterRecord value, OutputStream outStream) throws CoderException, IOException {
            StringUtf8Coder.of().encode(value.username, outStream);
            StringUtf8Coder.of().encode(value.tweet, outStream);
            VarInt.encode(value.timestamp, outStream);
        }

        @Override
        public TwitterRecord decode(InputStream inStream) throws CoderException, IOException {
            final String username = StringUtf8Coder.of().decode(inStream);
            final String tweet = StringUtf8Coder.of().decode(inStream);
            final Long timestamp = VarInt.decodeLong(inStream);

            return new TwitterRecord(
                    username,
                    tweet,
                    timestamp
            );
        }

        @Override
        public List<TwitterRecord.TwitterRecordCoder> getCoderArguments() {
            return new LinkedList<>();
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {
            //not used for demo
        }
    }
}
