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

  private static final Logger log = LoggerFactory.getLogger(PurgeOldPreferencesJob.class);

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    Components components = Components.instance();
    Storage storage = components.getDomainIndependetStorage();

    log.info("Purging of old preferences started.");
    try {

      storage.purgeOldPreferences();

    } catch (IOException e) {
      log.warn("Purging of old preferences failed!", e);
    }
    log.info("Purging of old preferences done.");
  }
}
