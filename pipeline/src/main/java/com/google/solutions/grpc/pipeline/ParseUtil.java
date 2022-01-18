/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.solutions.grpc.pipeline;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseUtil {
  private static final Pattern pattern = Pattern.compile("\\{\"zip\": \"([0-9]{5})\"\\}");
  public static String parseZipOutOfJsonPayload(String jsonPayload) {
    // Payload is very simple - {"zip": "60477"}, we just regex the data instead of parsing JSON.
    Matcher matcher = pattern.matcher(jsonPayload);
    if(matcher.find()) {
      return matcher.group(1);
    } else {
      throw new IllegalArgumentException("Payload doesn't match the expected pattern.");
    }
  }

}
