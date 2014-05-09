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

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spi.ThreadPool;

import java.io.Closeable;
import java.io.IOException;

/** a class to schedule the training of recommenders */
public class TrainingScheduler implements Closeable {

  private final Scheduler scheduler;

  public TrainingScheduler() throws Exception {
    ThreadPool threadPool = new SimpleThreadPool(1, Thread.NORM_PRIORITY);
    threadPool.initialize();

    DirectSchedulerFactory schedulerFactory = DirectSchedulerFactory.getInstance();
    schedulerFactory.createScheduler(threadPool, new RAMJobStore());

    scheduler = schedulerFactory.getScheduler();
  }

  public void start() {
    try {
      scheduler.start();
    } catch (SchedulerException e) {
      throw new RuntimeException(e);
    }
  }

  private JobKey key(String recommenderName) {
    return new JobKey("train-" + recommenderName);
  }

  public void addRecommenderTrainingJob(String recommenderName) {
    JobDetail job = JobBuilder.newJob(TrainRecommenderJob.class)
        .withIdentity(key(recommenderName))
        .build();
    job.getJobDataMap().put(TrainRecommenderJob.RECOMMENDER_NAME_PARAM, recommenderName);

    try {
      scheduler.addJob(job, true);
    } catch (SchedulerException e) {
      throw new RuntimeException(e);
    }
  }

  public void addRecommenderTrainingJobWithCronSchedule(String recommenderName, String cronExpression) {
    try {
      JobDetail job = JobBuilder.newJob(TrainRecommenderJob.class)
          .withIdentity(key(recommenderName))
          .build();
      job.getJobDataMap().put(TrainRecommenderJob.RECOMMENDER_NAME_PARAM, recommenderName);

      // http://www.quartz-scheduler.org/documentation/quartz-2.1.x/tutorials/crontrigger
      CronTrigger trigger = TriggerBuilder.newTrigger()
          .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
          .build();

      scheduler.scheduleJob(job, trigger);
    } catch (SchedulerException e) {
      throw new RuntimeException(e);
    }
  }

  public void immediatelyTrainRecommender(String recommenderName) throws SchedulerException {
      scheduler.triggerJob(key(recommenderName));
  }

  @Override
  public void close() throws IOException {
    try {
      scheduler.shutdown();
    } catch (SchedulerException e) {
      throw new IOException(e);
    }
  }

}
