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

import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.Factorization;
import org.apache.mahout.cf.taste.impl.recommender.svd.FilePersistenceStrategy;
import org.apache.mahout.cf.taste.model.DataModel;
import org.plista.kornakapi.core.config.FactorizationbasedRecommenderConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/** a {@link Trainer} for matrix factorization based recommenders */
public class FactorizationbasedInMemoryTrainer extends AbstractTrainer {

  private final FactorizationbasedRecommenderConfig conf;
  private static final Logger log = LoggerFactory.getLogger(FactorizationbasedInMemoryTrainer.class);

  public FactorizationbasedInMemoryTrainer(FactorizationbasedRecommenderConfig conf) {
    super(conf);
    this.conf = conf;
  }

  @Override
  protected void doTrain(File targetFile, DataModel inmemoryData, int numProcessors) throws IOException {
    try {
    	if(inmemoryData.getNumItems() >= 5 && inmemoryData.getNumUsers() >= 10){//preventing matrix singularity
	      ALSWRFactorizer factorizer = new ALSWRFactorizer(inmemoryData, conf.getNumberOfFeatures(), conf.getLambda(),
	          conf.getNumberOfIterations(), conf.isUsesImplicitFeedback(), conf.getAlpha(), numProcessors);
	      
	      long start = System.currentTimeMillis();
	      Factorization factorization = factorizer.factorize();
	      long estimateDuration = System.currentTimeMillis() - start;
	      
	      if (log.isInfoEnabled()) {
	    	  log.info("Model trained in {} ms", estimateDuration);
	      }
	
	      new FilePersistenceStrategy(targetFile).maybePersist(factorization);
    	}

    } catch (Exception e) {
      throw new IOException(e);
    }
  }

}
