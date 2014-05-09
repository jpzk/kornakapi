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

package org.plista.kornakapi.web.servlets;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.recommender.svd.Factorization;
import org.apache.mahout.cf.taste.impl.recommender.svd.FilePersistenceStrategy;
import org.apache.mahout.cf.taste.impl.recommender.svd.PersistenceStrategy;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.CandidateItemsStrategy;
import org.plista.kornakapi.core.config.Configuration;
import org.plista.kornakapi.core.config.FactorizationbasedRecommenderConfig;
import org.plista.kornakapi.core.config.RecommenderConfig;
import org.plista.kornakapi.core.recommender.CachingAllUnknownItemsCandidateItemsStrategy;
import org.plista.kornakapi.core.recommender.FoldingFactorizationBasedRecommender;
import org.plista.kornakapi.core.storage.CandidateCacheStorageDecorator;
import org.plista.kornakapi.core.storage.MySqlMaxPersistentStorage;
import org.plista.kornakapi.core.storage.MySqlStorage;
import org.plista.kornakapi.core.training.FactorizationbasedInMemoryTrainer;
import org.plista.kornakapi.core.training.preferencechanges.DelegatingPreferenceChangeListener;
import org.plista.kornakapi.core.training.preferencechanges.InMemoryPreferenceChangeListener;
import org.plista.kornakapi.web.Parameters;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;
import java.util.List;

/** servlet to manually trigger the training for a recommender */
public class TrainServlet extends BaseServlet {
	
	private static final Logger log = LoggerFactory.getLogger(TrainServlet.class);

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

	String label = getParameter(request, Parameters.LABEL, true);
    String recommenderName = getParameter(request, Parameters.RECOMMENDER, true)+"_"+ label;
    if(!containsTrainer(recommenderName)){
    	try {
			createRecommenderForLabel(label);
		} catch (TasteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	try {
		scheduler().immediatelyTrainRecommender(recommenderName);
	} catch (SchedulerException e) {
		e.printStackTrace();
	}
    

  }
  private void createRecommenderForLabel(String label) throws IOException, TasteException{
	  	Configuration conf = getConfiguration();
	  	List<FactorizationbasedRecommenderConfig> factorizationbasedConfs= conf.getFactorizationbasedRecommenders();
        FactorizationbasedRecommenderConfig factorizationbasedConf =factorizationbasedConfs.get(0);

        String name = factorizationbasedConf.getName() +"_"+ label;
        if(conf.getMaxPersistence()){
        	storages().put(label, new CandidateCacheStorageDecorator(new MySqlMaxPersistentStorage(conf.getStorageConfiguration(), label)));
        }else{
      		storages().put(label,  new CandidateCacheStorageDecorator(new MySqlStorage(conf.getStorageConfiguration(), label)));

        }

        File modelFile = new File(conf.getModelDirectory(), name + ".model");
 
        PersistenceStrategy persistence = new FilePersistenceStrategy(modelFile);

        if (!modelFile.exists()) {
          createEmptyFactorization(persistence);
        }

        DataModel persistenData = storages().get(label).recommenderData();
        
        CandidateItemsStrategy allUnknownItemsStrategy =
            new CachingAllUnknownItemsCandidateItemsStrategy(persistenData);

        FoldingFactorizationBasedRecommender svdRecommender = new FoldingFactorizationBasedRecommender(persistenData,
            allUnknownItemsStrategy, persistence);

        setRecommender(name, svdRecommender);
        setTrainer(name, new FactorizationbasedInMemoryTrainer(factorizationbasedConf));

        String cronExpression = factorizationbasedConf.getRetrainCronExpression();
        if (cronExpression == null) {
        	scheduler().addRecommenderTrainingJob(name);
        } else {
        	scheduler().addRecommenderTrainingJobWithCronSchedule(name, cronExpression);
        	try {
				scheduler().immediatelyTrainRecommender(name);
			} catch (SchedulerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }

        if (factorizationbasedConf.getRetrainAfterPreferenceChanges() !=
            RecommenderConfig.DONT_RETRAIN_ON_PREFERENCE_CHANGES) {
        	((DelegatingPreferenceChangeListener)preferenceChangeListener()).addDelegate(new InMemoryPreferenceChangeListener(scheduler(), name,
              factorizationbasedConf.getRetrainAfterPreferenceChanges()));
        }

        log.info("Added FactorizationBasedRecommender [{}] using [{}] features and [{}] iterations for label: {}",
            new Object[] { name, factorizationbasedConf.getNumberOfFeatures(),
                factorizationbasedConf.getNumberOfIterations() , label});
  }
  private void createEmptyFactorization(PersistenceStrategy strategy) throws IOException {
	    strategy.maybePersist(new Factorization(new FastByIDMap<Integer>(0), new FastByIDMap<Integer>(0),
	        new double[0][0], new double[0][0]));
	  }
}