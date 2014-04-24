package org.plista.kornakapi.core.training;

import org.plista.kornakapi.core.storage.Storage;
import org.plista.kornakapi.web.Components;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PurgeOldPreferencesJob implements Job {

  public static final String PURGE_OLDERTHANHOURS_PARAM = PurgeOldPreferencesJob.class.getName() + ".olderThanHours";

  private static final Logger log = LoggerFactory.getLogger(PurgeOldPreferencesJob.class);

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {

    Components components = Components.instance();

    int olderThanHours = context.getJobDetail().getJobDataMap().getInt(PURGE_OLDERTHANHOURS_PARAM);

    Storage storage = components.storage();

    log.info("Purging of old preferences started.");
    try {

      if (olderThanHours <= 0) {
        throw new IllegalStateException("Cannot purge with non-positive older-than-hours setting!");
      }

      storage.purgePreferences(olderThanHours);

    } catch (IOException e) {
      log.warn("Purging of old preferences failed!", e);
    }
    log.info("Purging of old preferences done.");
  }
}
