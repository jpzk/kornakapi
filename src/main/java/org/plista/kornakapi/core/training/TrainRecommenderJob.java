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

import org.plista.kornakapi.web.Components;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/** a {@link Job} to train a recommender */
public class TrainRecommenderJob implements Job {

  public static final String RECOMMENDER_NAME_PARAM = TrainRecommenderJob.class.getName() + ".recommenderName";

  private static final Logger log = LoggerFactory.getLogger(TrainRecommenderJob.class);

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {

    Components components = Components.instance();

    String recommenderName = context.getJobDetail().getJobDataMap().getString(RECOMMENDER_NAME_PARAM);
    String label = recommenderName.substring(recommenderName.indexOf("_")+1);

    Trainer trainer = components.trainer(recommenderName);

    log.info("Training for recommender [{}] started.", recommenderName);
    try {
      trainer.train(new File(components.getConfiguration().getModelDirectory()), components.storages().get(label),
          components.recommender(recommenderName), components.getConfiguration().getNumProcessorsForTraining(), recommenderName);
    } catch (IOException e) {
      log.warn("Training of recommender [" + recommenderName + "] failed!", e);
    }
    log.info("Training for recommender [{}] done.", recommenderName);
  }
}
