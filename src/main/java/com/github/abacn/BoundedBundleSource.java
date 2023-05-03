package com.github.abacn;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class BoundedBundleSource extends BoundedSource<byte[]> {
  private static final Logger LOG = LoggerFactory.getLogger(BoundedBundleSource.class);

  final int numBundle;

  final int numSourcePerBundle;

  final int numElePerSource;

  public BoundedBundleSource(int numBundle, int numSourcePerBundle, int numElePerSource) {
    this.numBundle = numBundle;
    this.numSourcePerBundle = numSourcePerBundle;
    this.numElePerSource = numElePerSource;
  }

  @Override
  public Coder<byte[]> getOutputCoder() {
    return ByteArrayCoder.of();
  }

  @Override
  public List<? extends BoundedSource<byte[]>> split(long desiredBundleSizeBytes, PipelineOptions options) throws Exception {

   ArrayList<SyntheticOffsetSource> sources = new ArrayList<>(numBundle);
   for(int i=0; i<numBundle; ++i) {
     sources.add(new SyntheticOffsetSource(numElePerSource, ImmutableList.copyOf(LongStream.range(0, numSourcePerBundle).boxed().collect(Collectors.toList()))));
   }
   return sources;
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return 0;
  }

  @Override
  public BoundedReader<byte[]> createReader(PipelineOptions options) throws IOException {
    throw new UnsupportedOperationException("must split");
  }
}
