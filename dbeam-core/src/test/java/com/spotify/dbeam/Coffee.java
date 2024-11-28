/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.dbeam;

import com.google.auto.value.AutoValue;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

// A fictitious DB model to test different SQL types
@AutoValue
public abstract class Coffee {

  public static Coffee create(
      final String name,
      final Optional<Integer> supId,
      final BigDecimal price,
      final Float temperature,
      final Double size,
      final Boolean isArabic,
      final Integer sales,
      final Long total,
      final java.sql.Timestamp created,
      final Optional<java.sql.Timestamp> updated,
      final UUID uid,
      final Long rownum,
      final List<Integer> intArr,
      final List<String> textArr) {
    return new AutoValue_Coffee(
        name,
        supId,
        price,
        temperature,
        size,
        isArabic,
        sales,
        total,
        created,
        updated,
        uid,
        rownum,
        new ArrayList<>(intArr),
        new ArrayList<>(textArr));
  }

  public abstract String name();

  public abstract Optional<Integer> supId();

  public abstract BigDecimal price();

  public abstract Float temperature();

  public abstract Double size();

  public abstract Boolean isArabic();

  public abstract Integer sales();

  public abstract Long total();

  public abstract java.sql.Timestamp created();

  public abstract Optional<java.sql.Timestamp> updated();

  public abstract UUID uid();

  public abstract Long rownum();

  public abstract List<Integer> intArr();

  public abstract List<String> textArr();

  public String insertStatement() {
    return String.format(
        Locale.ENGLISH,
        "INSERT INTO COFFEES " + "VALUES ('%s', %s, '%s', %f, %f, %b, %d, %d, '%s', %s, '%s', %d,"
        + " ARRAY [%s], ARRAY ['%s'])",
        name(),
        supId().orElse(null),
        price().toString(),
        temperature(),
        size(),
        isArabic(),
        sales(),
        total(),
        created(),
        updated().orElse(null),
        uid(),
        rownum(),
        String.join(",", intArr().stream().map(x -> (CharSequence)x.toString())::iterator),
        String.join("','", textArr()));
  }

  public static String ddl() {
    return "DROP TABLE IF EXISTS COFFEES; "
        + "CREATE TABLE COFFEES ("
        + "\"COF_NAME\" VARCHAR NOT NULL PRIMARY KEY,"
        + "\"SUP_ID\" INTEGER,"
        + "\"PRICE\" DECIMAL(21,2) NOT NULL,"
        + "\"TEMPERATURE\" REAL NOT NULL,"
        + "\"SIZE\" DOUBLE NOT NULL,"
        + "\"IS_ARABIC\" BOOLEAN NOT NULL,"
        + "\"SALES\" INTEGER DEFAULT 0 NOT NULL,"
        + "\"TOTAL\" BIGINT DEFAULT 0 NOT NULL,"
        + "\"CREATED\" TIMESTAMP NOT NULL,"
        + "\"UPDATED\" TIMESTAMP,"
        + "\"UID\" UUID NOT NULL,"
        + "\"ROWNUM\" BIGINT NOT NULL,"
        + "\"INT_ARR\" INTEGER ARRAY NOT NULL,"
        + "\"TEXT_ARR\" VARCHAR ARRAY NOT NULL);";
  }

  public static Coffee COFFEE1 =
      create(
          "costa rica caffee",
          Optional.empty(),
          new BigDecimal("7.20"),
          82.5f,
          320.7,
          true,
          17,
          200L,
          new java.sql.Timestamp(1488300933000L),
          Optional.empty(),
          UUID.fromString("123e4567-e89b-12d3-a456-426655440000"),
          1L,
          new ArrayList<Integer>() {{
            add(5);
            add(7);
            add(11);
          }},
          new ArrayList<String>() {{
            add("rock");
            add("scissors");
            add("paper");
          }});
  public static Coffee COFFEE2 =
      create(
          "colombian caffee",
          Optional.empty(),
          new BigDecimal("9.20"),
          87.5f,
          230.7,
          true,
          13,
          201L,
          new java.sql.Timestamp(1488300723000L),
          Optional.empty(),
          UUID.fromString("123e4567-e89b-a456-12d3-426655440000"),
          2L,
          new ArrayList<Integer>() {{
            add(7);
            add(11);
            add(23);
          }},
          new ArrayList<String>() {{
            add("scissors");
            add("paper");
            add("rock");
          }});
}
