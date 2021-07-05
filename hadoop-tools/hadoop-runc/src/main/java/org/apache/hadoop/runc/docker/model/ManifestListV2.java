/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.runc.docker.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ManifestListV2 {

  public static final String CONTENT_TYPE =
      "application/vnd.docker.distribution.manifest.list.v2+json";

  public static boolean matches(String contentType) {
    return CONTENT_TYPE.equals(contentType);
  }

  private String digest;

  private int schemaVersion;
  private String mediaType;
  private List<ManifestRefV2> manifests = new ArrayList<>();

  @JsonProperty
  public int getSchemaVersion() {
    return schemaVersion;
  }

  public void setSchemaVersion(int schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  @JsonProperty
  public String getMediaType() {
    return mediaType;
  }

  public void setMediaType(String mediaType) {
    this.mediaType = mediaType;
  }

  @JsonProperty
  public List<ManifestRefV2> getManifests() {
    return manifests;
  }

  @Override
  public String toString() {
    return String.format(
        "{%n"
            + "  schemaVersion=%d,%n"
            + "  mediaType=%s,%n"
            + "  manifests=%s%n"
            + "}",
        schemaVersion,
        mediaType,
        manifests
            .stream()
            .map(Objects::toString)
            .collect(Collectors.joining(",\n    ", "[\n    ", "\n  ]")));
  }

}
