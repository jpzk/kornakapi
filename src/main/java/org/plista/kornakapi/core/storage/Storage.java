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

package org.plista.kornakapi.core.storage;

import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.plista.kornakapi.core.Candidate;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/** I/O operations for the weblayer */
public interface Storage extends Closeable {

  DataModel trainingData() throws IOException;

  DataModel recommenderData() throws IOException;

  void setPreference(long userID, long itemID, float value) throws IOException;

  void batchSetPreferences(Iterator<Preference> preferences, int batchSize) throws IOException;

  void purgeOldPreferences() throws IOException;

  void addCandidate(String label, long itemID) throws IOException;

  Iterable<String> batchAddCandidates(Iterator<Candidate> candidates, int batchSize) throws IOException;

  void deleteCandidate(String label, long itemID) throws IOException;

  Iterable<String> batchDeleteCandidates(Iterator<Candidate> candidates, int batchSize) throws IOException;

  void deleteAllCandidates(String label) throws IOException;

  FastIDSet getCandidates(String label) throws IOException;
}
