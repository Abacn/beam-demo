package com.github.abacn.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableMap;

public class JsonTest {

  public static void main(String[] argv) {
    ImmutableMap<String, String> map = ImmutableMap.<String, String>builder()
        .put("normal", "{\"a\": 1}")
        .put("ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER", "{\"a\": \"\\1\"}")
        .put("ALLOW_COMMENTS", "{\"a\": \"1\" // has comment\n}")
        .put("ALLOW_MISSING_VALUES", "{\"a\": [1,,2]}")
        .put("ALLOW_NON_NUMERIC_NUMBERS", "{\"a\": NaN}")
        .put("ALLOW_NUMERIC_LEADING_ZEROS", "{\"a\": 0123}")
        .put("ALLOW_SINGLE_QUOTES", "{\"a\": 'abc'}")
        .put("ALLOW_TRAILING_COMMA", "{\"a\": 1, }")
        .put("ALLOW_UNQUOTED_CONTROL_CHARS", "{a: 1\n}")
        .put("ALLOW_UNQUOTED_FIELD_NAMES", "{a: 1}")
        .put("ALLOW_YAML_COMMENTS", "{\"a\": \"\\1\" # has comment\n}")
        .build();

    ObjectMapper mapper = new ObjectMapper();
    ObjectMapper relaxMapper = JsonMapper.builder().enable(
JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER,
        JsonReadFeature.ALLOW_JAVA_COMMENTS,
        JsonReadFeature.ALLOW_MISSING_VALUES,
        JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS,
        JsonReadFeature.ALLOW_LEADING_ZEROS_FOR_NUMBERS,
        JsonReadFeature.ALLOW_SINGLE_QUOTES,
        JsonReadFeature.ALLOW_TRAILING_COMMA,
        JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS,
        JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES,
        JsonReadFeature.ALLOW_YAML_COMMENTS).build();

    map.forEach(
        (k, v) -> {
          char fastjson, jackson, jackson2;
          try {
            JSONObject x = JSON.parseObject(v);
            // System.err.println(x);
            fastjson = 'o';
          } catch (JSONException e) {
            fastjson = 'x';
          }

          try {
            mapper.readTree(v);
            jackson = 'o';
          } catch (JsonProcessingException e) {
            jackson = 'x';
          }

          try {
            relaxMapper.readTree(v);
            jackson2 = 'o';
          } catch (JsonProcessingException e) {
            jackson2 = 'x';
          }

          System.out.printf("| %s | %c | %c | %c |\n", k, fastjson, jackson, jackson2);
        }
    );
  }
}