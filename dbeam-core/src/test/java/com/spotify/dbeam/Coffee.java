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
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

// A fictitious DB model to test different SQL types
@AutoValue
public abstract class Coffee {

  public static Coffee create(String name, Optional<Integer> supId, BigDecimal price,
                              Float temperature, Double size, Boolean isArabic, Integer sales,
                              Long total, java.sql.Timestamp created,
                              Optional<java.sql.Timestamp> updated,
                              UUID uid, Long rownum) {
    return new AutoValue_Coffee(name, supId, price, temperature, size, isArabic, sales,
                                total, created, updated, uid, rownum);
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

  public String insertStatement() {
    return String.format(
        Locale.ENGLISH,
        "INSERT INTO COFFEES "
        + "VALUES ('%s', %s, '%s', %f, %f, %b, %d, %d, '%s', %s, '%s', %d)",
        name(), supId().orElse(null), price().toString(), temperature(), size(), isArabic(),
        sales(), total(), created(), updated().orElse(null), uid(), rownum());
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
           + "\"ROWNUM\" BIGINT NOT NULL);";
  }

  public static Coffee COFFEE1 = create(
      "costa rica caffee", Optional.empty(), new BigDecimal("7.20"), 82.5f,
      320.7, true, 17, 200L, new java.sql.Timestamp(1488300933000L),
      Optional.empty(), UUID.fromString("123e4567-e89b-12d3-a456-426655440000"), 1L
  );
  public static Coffee COFFEE2 = create(
      "colombian caffee", Optional.empty(), new BigDecimal("9.20"), 87.5f,
      230.7, true, 13, 201L, new java.sql.Timestamp(1488300723000L),
      Optional.empty(), UUID.fromString("123e4567-e89b-a456-12d3-426655440000"), 2L
  );

}
