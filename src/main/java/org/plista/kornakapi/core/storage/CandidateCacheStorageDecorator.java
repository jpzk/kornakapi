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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.plista.kornakapi.core.Candidate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * a decorator for a {@link Storage} that caches the underlying candidate sets
 *
 * http://code.google.com/p/guava-libraries/wiki/CachesExplained
 */
public class CandidateCacheStorageDecorator implements Storage {

  private final Storage delegate;
  private final Cache<String, FastIDSet> cache;

  private static final Logger log = LoggerFactory.getLogger(CandidateCacheStorageDecorator.class);

  public CandidateCacheStorageDecorator(Storage delegate) {
    this.delegate = delegate;
    cache = CacheBuilder.newBuilder()
        .maximumSize(100)
        .build();
  }

  @Override
  public DataModel trainingData() throws IOException {
    return delegate.trainingData();
  }

  @Override
  public DataModel recommenderData() throws IOException {
    return delegate.recommenderData();
  }

  @Override
  public void setPreference(long userID, long itemID, float value) throws IOException {
    delegate.setPreference(userID, itemID, value);
  }

  @Override
  public void batchSetPreferences(Iterator<Preference> preferences, int batchSize) throws IOException {
    delegate.batchSetPreferences(preferences, batchSize);
  }

  private void invalidateCache(String label) {
    cache.invalidate(label);
    if (log.isInfoEnabled()) {
      log.info("Invalidated cache for label {}", label);
    }
  }

  @Override
  public void addCandidate(String label, long itemID) throws IOException {
    delegate.addCandidate(label, itemID);
    invalidateCache(label);
  }

  @Override
  public Iterable<String> batchAddCandidates(Iterator<Candidate> candidates, int batchSize) throws IOException {
    Iterable<String> modifiedLabels = delegate.batchAddCandidates(candidates, batchSize);

    for (String label : modifiedLabels) {
      invalidateCache(label);
    }

    return modifiedLabels;
  }

  @Override
  public void deleteCandidate(String label, long itemID) throws IOException {
    delegate.deleteCandidate(label, itemID);
    invalidateCache(label);
  }

  @Override
  public Iterable<String> batchDeleteCandidates(Iterator<Candidate> candidates, int batchSize) throws IOException {
    Iterable<String> modifiedLabels = delegate.batchDeleteCandidates(candidates, batchSize);
    for (String label : modifiedLabels) {
      invalidateCache(label);
    }
    return modifiedLabels;
  }

  @Override
  public void deleteAllCandidates(String label) throws IOException {
    delegate.deleteAllCandidates(label);
    invalidateCache(label);
  }
  
  public LinkedList<String> getAllLabels(){
	  LinkedList<String> allLabels = null;
	  if(delegate instanceof MySqlStorage){
		  try {
			  allLabels = ((MySqlStorage) delegate).getAllLabels();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }
	  return allLabels;
	}
  
  public String getItemsLabel(Long itemID) throws IOException{
	  String itemLabel = null;
	  if(delegate instanceof MySqlStorage){
		  itemLabel = ((MySqlStorage) delegate).getItemsLabel(itemID);
	  }
	  return itemLabel;
  }
	  

  //TODO ideally other calls with to an equal uncached label should block and wait for a single retrieval
  @Override
  public FastIDSet getCandidates(String label) throws IOException {
    FastIDSet candidates = cache.getIfPresent(label);

    if (candidates == null) {
      candidates = delegate.getCandidates(label);
      cache.put(label, candidates);

      if (log.isInfoEnabled()) {
        log.info("Caching {} candidates for label {}", candidates.size(), label);
      }
    }

    return candidates;
  }

  @Override
  public void close() throws IOException {
    cache.cleanUp();
    delegate.close();
  }
}
