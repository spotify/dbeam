/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2023 Spotify AB
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

package com.spotify.dbeam.avro;

public class TestAvroSchemas {

  public static String getSchemaWithMissingField() {
    return "{\n"
        + "  \"type\" : \"record\",\n"
        + "  \"name\" : \"big_numbers_record\",\n"
        + "  \"namespace\" : \"com.acompany.data.schemas\",\n"
        + "  \"doc\" : \"Daily data dump for big_numbers table\",\n"
        + "  \"fields\" : [\n"
        + "  {\n"
        + "    \"name\" : \"cof_name\",\n"
        + "    \"type\" : \"string\",\n"
        + "    \"doc\" : \"Unique identifier. #ID_L1\"\n"
        + "  }\n"
        + "  ]\n"
        + "}";
  }

  public static String getSchemaWithFieldsInWrongOrder() {
    return "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"user_sql_query\",\n"
        + "  \"namespace\": \"dbeam_generated\",\n"
        + "  \"doc\": \"Dummy doc\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"TOTAL\",\n"
        + "      \"type\": [\n"
        + "        \"null\",\n"
        + "        \"long\"\n"
        + "      ],\n"
        + "      \"doc\": \"Dummy\",\n"
        + "      \"default\": null\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"COF_NAME\",\n"
        + "      \"type\": [\n"
        + "        \"null\",\n"
        + "        \"string\"\n"
        + "      ],\n"
        + "      \"doc\": \"Dummy\",\n"
        + "      \"default\": null\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"SIZE\",\n"
        + "      \"type\": [\n"
        + "        \"null\",\n"
        + "        \"double\"\n"
        + "      ],\n"
        + "      \"doc\": \"Dummy\",\n"
        + "      \"default\": null\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";
  }
}
