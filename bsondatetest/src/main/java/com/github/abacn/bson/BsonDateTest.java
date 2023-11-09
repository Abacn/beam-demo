package com.github.abacn.bson;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.bson.Document;

import java.time.Instant;
import java.util.Date;

public class BsonDateTest {
  static final Gson GSON = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ssSSSSZ").create();
  public static void main(String[] argv) {
    Document document = new Document();
    document.put("date", new Date());



    System.out.println(GSON.toJson(document));
    System.out.println(Instant.now());

    document.forEach(
        (key, value) -> {
          System.out.printf("%s: %s\n", key, value.toString());
        });
  }
}
