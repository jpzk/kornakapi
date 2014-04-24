/**
 * Copyright 2012 plista GmbH  (http://www.plista.com/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 */

package org.plista.kornakapi.core.config;

/** configuration for the database backend */
public class StorageConfiguration {

  private String jdbcDriverClass;
  private String jdbcUrl;
  private String username;
  private String password;
  private String purgePreferencesCronExpression;
  private int purgePreferencesOlderThanHours;

  public String getJdbcDriverClass() {
    return jdbcDriverClass;
  }

  public void setJdbcDriverClass(String jdbcDriverClass) {
    this.jdbcDriverClass = jdbcDriverClass;
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public void setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getPurgePreferencesCronExpression() {
    return purgePreferencesCronExpression;
  }

  public void setPurgePreferencesCronExpression(String purgePreferencesCronExpression) {
    this.purgePreferencesCronExpression = purgePreferencesCronExpression;
  }

  public int getPurgePreferencesOlderThanHours() {
    return purgePreferencesOlderThanHours;
  }

  public void setPurgePreferencesOlderThanHours(int purgePreferencesOlderThanHours) {
    this.purgePreferencesOlderThanHours = purgePreferencesOlderThanHours;
  }
}
