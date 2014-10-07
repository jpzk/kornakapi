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

package org.plista.kornakapi.web;

/** GET parameters for the application */
public class Parameters {

  public static final String RECOMMENDER = "recommender";
  public static final String USER_ID = "userID";
  public static final String ITEM_IDS = "itemIDs";
  public static final String ITEM_ID = "itemID";
  public static final String VALUE = "value";
  public static final String HOW_MANY = "howMany";
  public static final String FILE = "file";
  public static final String BATCH_SIZE = "batchSize";
  public static final String LABEL = "label";
  public static final String Text = "text";

  public static final int DEFAULT_HOW_MANY = 10;
  public static final int DEFAULT_BATCH_SIZE = 10000;

  private Parameters() {
    throw new IllegalStateException();
  }
}
