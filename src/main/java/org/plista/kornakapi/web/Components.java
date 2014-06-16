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

import com.google.common.base.Preconditions;

import org.apache.commons.dbcp.BasicDataSource;
import org.plista.kornakapi.KornakapiRecommender;
import org.plista.kornakapi.core.config.Configuration;
import org.plista.kornakapi.core.storage.CandidateCacheStorageDecorator;
import org.plista.kornakapi.core.storage.MySqlStorage;
import org.plista.kornakapi.core.training.Trainer;
import org.plista.kornakapi.core.training.TrainingScheduler;
import org.plista.kornakapi.core.training.preferencechanges.DelegatingPreferenceChangeListenerForLabel;


import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/** all singleton instances used in the application, used for dependency injection */
public class Components {

  private final Configuration conf;
  private final HashMap<String, CandidateCacheStorageDecorator> storages;
  private final Map<String, KornakapiRecommender> recommenders;
  private final Map<String, Trainer> trainers;
  private final TrainingScheduler scheduler;
  private final DelegatingPreferenceChangeListenerForLabel preferenceChangeListener;
  private final LinkedList<String> labels;
  private final BasicDataSource dataSource;
  private final CandidateCacheStorageDecorator domainIndependetStorage;

  private static Components INSTANCE;

  private Components(Configuration conf, HashMap<String,CandidateCacheStorageDecorator> storages, Map<String, KornakapiRecommender> recommenders,
        Map<String, Trainer> trainers, TrainingScheduler scheduler, DelegatingPreferenceChangeListenerForLabel preferenceChangeListener2, LinkedList<String>labels, BasicDataSource dataSource, CandidateCacheStorageDecorator domainIndependetStorage) {
    this.conf = conf;
    this.storages = storages;
    this.recommenders = recommenders;
    this.trainers = trainers;
    this.scheduler = scheduler;
    this.preferenceChangeListener = preferenceChangeListener2;
    this.labels = labels;
    this.dataSource = dataSource;
    this.domainIndependetStorage = domainIndependetStorage;
  }

  public static synchronized void init(Configuration conf, HashMap<String,CandidateCacheStorageDecorator> storages,
      Map<String, KornakapiRecommender> recommenders, Map<String, Trainer> trainers, TrainingScheduler scheduler,
      DelegatingPreferenceChangeListenerForLabel preferenceChangeListener2, LinkedList<String> labels, BasicDataSource dataSource, CandidateCacheStorageDecorator domainIndependetStorage) {
    Preconditions.checkState(INSTANCE == null);
    INSTANCE = new Components(conf, storages, recommenders, trainers, scheduler, preferenceChangeListener2, labels, dataSource, domainIndependetStorage);
  }

  public static Components instance() {
    return Preconditions.checkNotNull(INSTANCE);
  }


  public Configuration getConfiguration() {
    return conf;
  }

  public KornakapiRecommender recommender(String name) {
    return recommenders.get(name);
  }
  
  public void setRecommender(String name, KornakapiRecommender recommender){
	  recommenders.put(name, recommender);
  }

  public Trainer trainer(String name) {
    return trainers.get(name);
  }
  
  public void setTrainer(String name, Trainer trainer){
	  trainers.put(name, trainer);
  }

  public HashMap<String, CandidateCacheStorageDecorator> storages() {
    return storages;
  }
  public CandidateCacheStorageDecorator getDomainIndependetStorage(){
	  return this.domainIndependetStorage;
  }

  public TrainingScheduler scheduler() {
    return scheduler;
  }

  public DelegatingPreferenceChangeListenerForLabel preferenceChangeListener() {
    return preferenceChangeListener;
  }
  
  public BasicDataSource getDataSource(){
	  return dataSource;
  }
  
  public LinkedList<String> getLabels(){
	  return this.labels;
  }
}
