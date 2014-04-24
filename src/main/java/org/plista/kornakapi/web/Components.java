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
import org.plista.kornakapi.KornakapiRecommender;
import org.plista.kornakapi.core.config.Configuration;
import org.plista.kornakapi.core.storage.Storage;
import org.plista.kornakapi.core.training.TaskScheduler;
import org.plista.kornakapi.core.training.Trainer;
import org.plista.kornakapi.core.training.preferencechanges.PreferenceChangeListener;

import java.util.Map;

/** all singleton instances used in the application, used for dependency injection */
public class Components {

  private final Configuration conf;
  private final Storage storage;
  private final Map<String, KornakapiRecommender> recommenders;
  private final Map<String, Trainer> trainers;
  private final TaskScheduler scheduler;
  private final PreferenceChangeListener preferenceChangeListener;

  private static Components INSTANCE;

  private Components(Configuration conf, Storage storage, Map<String, KornakapiRecommender> recommenders,
        Map<String, Trainer> trainers, TaskScheduler scheduler, PreferenceChangeListener preferenceChangeListener) {
    this.conf = conf;
    this.storage = storage;
    this.recommenders = recommenders;
    this.trainers = trainers;
    this.scheduler = scheduler;
    this.preferenceChangeListener = preferenceChangeListener;
  }

  public static synchronized void init(Configuration conf, Storage storage,
      Map<String, KornakapiRecommender> recommenders, Map<String, Trainer> trainers, TaskScheduler scheduler,
      PreferenceChangeListener preferenceChangeListener) {
    Preconditions.checkState(INSTANCE == null);
    INSTANCE = new Components(conf, storage, recommenders, trainers, scheduler, preferenceChangeListener);
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

  public Trainer trainer(String name) {
    return trainers.get(name);
  }

  public Storage storage() {
    return storage;
  }

  public TaskScheduler scheduler() {
    return scheduler;
  }

  public PreferenceChangeListener preferenceChangeListener() {
    return preferenceChangeListener;
  }
}
