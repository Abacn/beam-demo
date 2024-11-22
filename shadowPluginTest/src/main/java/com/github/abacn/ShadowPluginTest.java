package com.github.abacn;

import org.apache.beam.sdk.providers.GenerateSequenceSchemaTransformProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

public class ShadowPluginTest {
  public static final Schema OUTPUT_SCHEMA = Schema.builder().addInt64Field("value").build();
  public static void main(String[] argv) throws ClassNotFoundException {
    Row unused = Row.withSchema(GenerateSequenceSchemaTransformProvider.OUTPUT_SCHEMA).withFieldValue("value", 1L).build();

    Class yourClass =  Class.forName("org.antlr.v4.runtime.CharStream");
  }
}
