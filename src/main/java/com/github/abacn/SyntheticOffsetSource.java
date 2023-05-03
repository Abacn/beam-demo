package com.github.abacn;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.OffsetBasedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

/** validate the design of BigQueryStorageStreamBundleSource */
public class SyntheticOffsetSource extends OffsetBasedSource<byte[]> {

  private static final Logger LOG =
          LoggerFactory.getLogger(SyntheticOffsetSource.class);
  public SyntheticOffsetSource(int numElePerSource, List<Long> seeds) {
    super(0, seeds.size(), 1L);
    if (seeds.size() > 0) {
      LOG.info("Create SyntheticOffsetSource of seeds idx: {} size: {}", seeds.get(0), seeds.size());
    }
    this.seeds = seeds;
    this.numElePerSource = numElePerSource;
  }

  @Override
  public long getMaxEndOffset(PipelineOptions options) throws Exception {
    return getEndOffset();
  }

  @Override
  public OffsetBasedSource<byte[]> createSourceForSubrange(long start, long end) {
    LOG.debug("Create createSourceForSubrange of {} {}", start, end);
    return new SyntheticOffsetSource(numElePerSource, seeds.subList((int) start, (int) end));
  }

  @Override
  public  BoundedReader<byte[]> createReader( PipelineOptions options) throws IOException {
    return new SyntheticOffsetReader(this);
  }

  @Override
  public Coder<byte[]> getOutputCoder() {
    return ByteArrayCoder.of();
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) {
    // The size of stream source can't be estimated due to server-side liquid sharding.
    // TODO: Implement progress reporting.
    return 0L;
  }

  @Override
  public List<? extends OffsetBasedSource<byte[]>> split(long desiredBundleSizeBytes, PipelineOptions options) {
    // no-op
    return ImmutableList.of(this);
  }

  List<Long> seeds;

  int numElePerSource;

  public static class SyntheticOffsetReader extends OffsetBasedReader<byte[]> {

    private int currentOffset;

    private boolean isNewStream;

    byte[] current;

    Iterator<byte[]> currentIterator;

    public SyntheticOffsetReader(OffsetBasedSource<byte[]> source) {
      super(source);
      this.currentOffset = 0;
    }

    @Override
    protected  long getCurrentOffset() throws NoSuchElementException {
      return currentOffset;
    }

    @Override
    protected boolean isAtSplitPoint() throws NoSuchElementException {
      return isNewStream;
    }

    @Override
    protected  boolean startImpl() throws IOException {
      return readNextStream();
    }

    @Override
    protected  boolean advanceImpl() throws IOException {
      isNewStream = false;
      return readNextRecord();
    }

    @Override
    public byte[] getCurrent() throws NoSuchElementException {
      return current;
    }

    @Override
    public void close() throws IOException {
      // do nothing
    }

    private boolean readNextStream() {
      SyntheticOffsetSource source = (SyntheticOffsetSource) getCurrentSource();
      if (currentOffset >= source.seeds.size()) {
        LOG.info("Current reader {} reached end.", source.seeds.get(source.seeds.size() - 1));
        currentIterator = null;
        return false;
      }
      currentIterator = new BytesIterator(source.seeds.get(currentOffset), source.numElePerSource);
      isNewStream = true;
      return readNextRecord();
    }

    private boolean readNextRecord() {
      if (currentIterator == null) {
        return false;
      }
      else if (!currentIterator.hasNext()) {
        ++currentOffset;
        return readNextStream();
      } else {
        current = currentIterator.next();
        return true;
      }
    }
  }

  static class BytesIterator implements Iterator<byte[]> {
    Random rand;
    int currentCount;
    final int maxCount;
    public BytesIterator(long seed, int numElePerSource) {
      rand = new Random(seed);
      maxCount = numElePerSource;
      currentCount = 0;
    }

    @Override
    public boolean hasNext() {
      return currentCount < maxCount;
    }

    @Override
    public byte[] next() {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      byte[] b = new byte[1000];
      rand.nextBytes(b);
      ++currentCount;
      return b;
    }
  }
}
