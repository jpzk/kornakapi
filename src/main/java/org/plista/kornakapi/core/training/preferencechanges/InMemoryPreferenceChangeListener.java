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

package org.plista.kornakapi.core.training.preferencechanges;

import com.google.common.base.Preconditions;
import org.quartz.SchedulerException;

import org.plista.kornakapi.core.training.TaskScheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**  a simple, transient {@link PreferenceChangeListener} */
public class InMemoryPreferenceChangeListener implements PreferenceChangeListener{

  private final TaskScheduler scheduler;
  private final String recommenderName;
  private final int retrainCount;
  private final AtomicLong numberOfChanges = new AtomicLong(0);

  private static final Logger log = LoggerFactory.getLogger(InMemoryPreferenceChangeListener.class);

  public InMemoryPreferenceChangeListener(TaskScheduler scheduler, String recommenderName, int retrainCount) {
    Preconditions.checkArgument(retrainCount > 0);
    this.scheduler = scheduler;
    this.recommenderName = recommenderName;
    this.retrainCount = retrainCount;
  }

  @Override
  public void notifyOfPreferenceChange() {
    long changes = numberOfChanges.incrementAndGet();
    if (changes % retrainCount == 0) {

      if (log.isInfoEnabled()) {
        log.info("Retraining recommender {} after {} preference changes", recommenderName, changes);
      }
      try {
		scheduler.immediatelyTrainRecommender(recommenderName);
	} catch (SchedulerException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    }
  }
}