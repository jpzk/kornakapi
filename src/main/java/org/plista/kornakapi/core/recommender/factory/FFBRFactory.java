package org.plista.kornakapi.core.recommender.factory;

import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.recommender.svd.Factorization;
import org.apache.mahout.cf.taste.impl.recommender.svd.FilePersistenceStrategy;
import org.apache.mahout.cf.taste.impl.recommender.svd.PersistenceStrategy;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.CandidateItemsStrategy;
import org.plista.kornakapi.core.config.Configuration;
import org.plista.kornakapi.core.config.FactorizationbasedRecommenderConfig;
import org.plista.kornakapi.core.recommender.CachingAllUnknownItemsCandidateItemsStrategy;
import org.plista.kornakapi.core.recommender.FoldingFactorizationBasedRecommender;


public class FFBRFactory {
	
	public FoldingFactorizationBasedRecommender getRecommender(Configuration conf, FactorizationbasedRecommenderConfig factorizationbasedConf, DataModel data, String label, String recommenderName) throws IOException, TasteException{


	        File modelFile = new File(conf.getModelDirectory(), recommenderName + ".model");

	        PersistenceStrategy persistence = new FilePersistenceStrategy(modelFile);

	        if (!modelFile.exists()) {
	          createEmptyFactorization(persistence);
	        }

	        CandidateItemsStrategy allUnknownItemsStrategy =
	            new CachingAllUnknownItemsCandidateItemsStrategy(data);

	        FoldingFactorizationBasedRecommender svdRecommender = new FoldingFactorizationBasedRecommender(data,
	            allUnknownItemsStrategy, persistence, factorizationbasedConf.getNumberOfThreadsForEstimation());
	        
	        return svdRecommender;

	}
	  private void createEmptyFactorization(PersistenceStrategy strategy) throws IOException {
		    strategy.maybePersist(new Factorization(new FastByIDMap<Integer>(0), new FastByIDMap<Integer>(0),
		        new double[0][0], new double[0][0]));
		  }

}
