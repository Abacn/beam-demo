package com.github.abacn;

import com.google.api.client.util.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* ./gradlew :icebergtest:run --args="--project=google.com:clouddfe --region=us-central1 --tempLocation=$TEMP_LOCATION --icebergTable=$ICEBERG_TABLE$(date +%s) --runner=DataflowRunner --apiRootUrl=https://dataflow-staging.sandbox.googleapis.com --experiments=enable_managed_transforms,use_runner_v2" -info
 */
public class IcebergTest {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergTest.class);
  private static final Integer NUM_RECORDS = 1000;

  private static final Schema SCHEMA =
      Schema.builder().addStringField("str").addInt64Field("number").build();
  private static final SimpleFunction<Long, Row> ROW_FUNC =
      new SimpleFunction<Long, Row>() {
        @Override
        public Row apply(Long input) {
          return Row.withSchema(SCHEMA).addValue("record_" + input).addValue(input).build();
        }
      };

  public interface Options extends PipelineOptions {
    @Description("Number of records that will be written and/or read by the test")
    @Default.Integer(1000)
    Integer getNumRecords();

    void setNumRecords(Integer numRecords);

    @Description("Number of shards in the test table")
    @Default.Integer(10)
    Integer getNumShards();

    void setNumShards(Integer numShards);

    @Default.String("mywarehouse1")
    String getWarehouse();

    void setWarehouse(String warehouse);

    @Default.String("test_catalog")
    String getCatalogName();

    void setCatalogName(String catalogName);

    String getIcebergTable();

    void setIcebergTable(String icebergTable);

    @Default.Boolean(false)
    Boolean getStreamingTest();

    void setStreamingTest(Boolean streamingTest);

    @Description(
        "Duration of the streaming source in seconds. If not specified, the test will select a"
            + " default value.")
    @Default.Integer(-1)
    Integer getStreamingSourceDuration();

    void setStreamingSourceDuration(Integer streamingSourceDuration);
  }

  static void createTable(Options options) throws IOException {
    Configuration catalogConf = new Configuration();
    catalogConf.set(
        "fs.gs.project.id",
        Preconditions.checkNotNull(
            options.as(GcpOptions.class).getProject(),
            "To create the table, please provide your GCP project using --project"));
    // catalogConf.set("fs.gs.auth.type", "APPLICATION_DEFAULT");

    String credentials = System.getenv("ICEBERG_GCP_CREDENTIALS_FILE");
    if (credentials == null) {
      throw new RuntimeException("ICEBERG_GCP_CREDENTIALS_FILE must be set");
    }
    catalogConf.set("fs.gs.auth.service.account.json.keyfile", credentials);

    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("warehouse", IcebergTest.getWarehouse(options))
            .build();

    HadoopCatalog catalog;
    catalog = new HadoopCatalog();
    catalog.setConf(catalogConf);
    catalog.initialize(options.getCatalogName(), properties);

    catalog.createTable(
        TableIdentifier.parse(options.getIcebergTable()),
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "str", Types.StringType.get()),
            Types.NestedField.required(2, "number", Types.LongType.get())));
    LOG.info("Created table: " + options.getIcebergTable());
  }

  static String getWarehouse(Options options) {
    return options.as(GcpOptions.class).getTempLocation() + File.separator + options.getWarehouse();
  }

  static void runIcebergWrite(IcebergTest.Options options) throws Exception {
    createTable(options);

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("Generate some longs", GenerateSequence.from(0).to(NUM_RECORDS))
        .apply("Convert longs to Beam Rows", MapElements.via(ROW_FUNC))
        .setRowSchema(SCHEMA)
        .apply(
            "Write to Iceberg",
            Managed.write(Managed.ICEBERG)
                .withConfig(
                    ImmutableMap.<String, Object>builder()
                        .put("table", options.getIcebergTable())
                        .put("catalog_name", options.getCatalogName())
                        .put(
                            "catalog_properties",
                            ImmutableMap.<String, Object>builder()
                                .put("catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
                                .put("warehouse", IcebergTest.getWarehouse(options))
                                .build())
                        .build()));

    pipeline.run().waitUntilFinish();
  }

  static PipelineResult runIcebergWriteStreaming(
      IcebergTest.Options options, boolean createTable)
      throws Exception {
    if (createTable) {
      createTable(options);
    }

    Pipeline pipeline = Pipeline.create(options);

    Integer streamingSourceDuration = options.getStreamingSourceDuration() * 1000;

    PCollection<Row> input =
        pipeline
            .apply("GenerateData", getStreamingSource(streamingSourceDuration))
            .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
            .apply(
                "ApplyMap",
                MapElements.into(TypeDescriptors.rows())
                    .via(instant -> ROW_FUNC.apply(instant.getMillis() % streamingSourceDuration)))
            .setRowSchema(SCHEMA);

    if (!input.isBounded().equals(PCollection.IsBounded.UNBOUNDED)) {
      throw new RuntimeException("Input is not unbounded");
    }

    input.apply(
        "WriteToIceberg",
        Managed.write(Managed.ICEBERG)
            .withConfig(
                ImmutableMap.<String, Object>builder()
                    .put("table", options.getIcebergTable())
                    .put("catalog_name", options.getCatalogName())
                    .put("triggering_frequency_seconds", 5)
                    .put(
                        "catalog_properties",
                        ImmutableMap.<String, Object>builder()
                            .put("catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
                            .put("warehouse", IcebergTest.getWarehouse(options))
                            .build())
                    .build()));

    return pipeline.run();
  }

  private static PeriodicImpulse getStreamingSource(Integer streamingSourceDuration) {
    return PeriodicImpulse.create()
        .stopAfter(Duration.millis(streamingSourceDuration))
        .withInterval(Duration.millis(100));
  }

  public static void main(String[] args) throws Exception {
    IcebergTest.Options icebergWriteOptions =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IcebergTest.Options.class);

    if (icebergWriteOptions.getStreamingTest()) {
      LOG.info("Running streaming test...");
      runIcebergWriteStreaming(icebergWriteOptions, true);
    } else {
      LOG.info("Running batch test...");
      runIcebergWrite(icebergWriteOptions);
    }
  }
}
