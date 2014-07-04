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

package org.plista.kornakapi.core.training;

import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.plista.kornakapi.core.config.RecommenderConfig;
import org.plista.kornakapi.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/** base class for all in-memory recommender trainers */
public abstract class AbstractTrainer implements Trainer {

  private final RecommenderConfig conf;
  private static final Logger log = LoggerFactory.getLogger(AbstractTrainer.class);

  protected AbstractTrainer(RecommenderConfig conf) {
    this.conf = conf;
  }

  @Override
  public void train(File modelDirectory, Storage storage, Recommender recommender, int numProcessors, String recommenderName)
      throws IOException {

    File targetFile = new File(modelDirectory, recommenderName + "-training.model");

    doTrain(targetFile, storage.trainingData(), numProcessors);

    boolean fileRenamed = targetFile.renameTo(new File(modelDirectory, recommenderName + ".model"));
    
    if (log.isInfoEnabled()) {
  	  log.info("Using new model {}", fileRenamed);
    }
    recommender.refresh(null);
  }

  protected abstract void doTrain(File targetFile, DataModel inmemoryData, int numProcessors) throws IOException;
}
