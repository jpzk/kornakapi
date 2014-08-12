package org.plista.kornakapi.core.optimizer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.recommender.svd.Factorization;
import org.apache.mahout.cf.taste.impl.recommender.svd.FilePersistenceStrategy;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.plista.kornakapi.core.config.Configuration;
import org.plista.kornakapi.core.storage.MySqlSplitableMaxPersistentStorage;
import org.plista.kornakapi.core.training.ALSWRFactorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FactorizationBasedInMemoryOptimizer extends AbstractOptimizer{
	
	private static final Logger log = LoggerFactory.getLogger(FactorizationBasedInMemoryOptimizer.class);	
	

	@Override
	public void optimize(File modelDirectory, Configuration conf, int numProcessors,  String label)
		throws IOException {
			
		int seed = 378934;
		int fold = 2;
		
		// 1 Generate split
		
		BasicDataSource dataSource = new BasicDataSource();
		MySqlSplitableMaxPersistentStorage data = new MySqlSplitableMaxPersistentStorage(conf.getStorageConfiguration(), label,dataSource, seed);
		ArrayList<DataModel> trainingSets = new ArrayList<DataModel>();
		ArrayList<DataModel> testSets = new ArrayList<DataModel>();
		
		trainingSets.add(data.trainingData(0));
		trainingSets.add(data.trainingData(1));
		trainingSets.add(data.trainingData(2));
		
		testSets.add(data.testData(0));
		testSets.add(data.testData(1));
		testSets.add(data.testData(2));
			
		ArrayList<Double> alphas = new ArrayList<Double>(Arrays.asList(190.0, 210.0,230.0,250.0,270.0,280.0,290.0,300.0,320.0,340.0,360.0));
		ArrayList<Double> lambdas = new ArrayList<Double>(Arrays.asList(1800.0,1900.0,2000.0,2100.0,2200.0,2300.0,2400.0,2500.0));
		ArrayList<Integer> features = new ArrayList<Integer>(Arrays.asList(5,1));
		ArrayList<Integer> iterations = new ArrayList<Integer>(Arrays.asList(10));
		
	    log.info("Starting Optimization");
		
		// 2 Loop over all hyperparameters
		for(int feature : features){
			for(double lambda : lambdas){
				for(double alpha : alphas){
					for(int iter : iterations){
						double error = 0;
							for(int i = 0; i<3; i++){
								Factorization factorization = null;
								try {
							      ALSWRFactorizer factorizer = new ALSWRFactorizer(trainingSets.get(i), feature, lambda,
							    		  iter, true,alpha, numProcessors);
							      
							      long start = System.currentTimeMillis();
							      factorization = factorizer.factorize();
							      long estimateDuration = System.currentTimeMillis() - start;
							      
							      if (log.isInfoEnabled()) {
							    	  log.info("Model trained in {} ms", estimateDuration);
							      }
							      File targetFile = new File("/opt/kornakapi-model/crossvalidation.model");
							
							      new FilePersistenceStrategy(targetFile).maybePersist(factorization);
						    }catch (Exception e) {
						      throw new IOException(e);
						    }
							// 3 measure performance
							    LongPrimitiveIterator userIDsIterator;
								try {
									userIDsIterator = testSets.get(i).getUserIDs();
						
		
								    while (userIDsIterator.hasNext()) {
								    	Long userID = userIDsIterator.next();
								    	double[] userf = null;
								    	PreferenceArray userPrefs = testSets.get(i).getPreferencesFromUser(userID);
								    	try{
								    		userf = factorization.getUserFeatures(userID);
								    	} catch(TasteException e){
								    		
								    	}
								    	if(userf != null){
									    	long[] itemIDs = userPrefs.getIDs();
									    	Vector userfVector = new DenseVector(userf);
									    	int idx = 0;
									    	for(long itemID: itemIDs ){
									    		double[] itemf = null;
									    		try{
									    			itemf = factorization.getItemFeatures(itemID);
									    		}catch(TasteException e){
									    			
									    		}
									    		if(itemf != null){
										    		Vector itemfVector = new DenseVector(itemf);
										    		double pref = itemfVector.dot(userfVector);
										    		double realpref = userPrefs.getValue(idx);
										    		idx++;
										    		error = error + Math.abs(pref - realpref); 
									    		}
		   		  
									    	}							    		
								    	}
		  
								    }
								    log.info("Error of {} for features {}, alpha {}, lambda {}, fold: {}", new Object[]{error, feature, alpha, lambda, i});
							    
							} catch (TasteException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}    
						}
							data.insertPerformance(label, feature, iter, alpha, lambda, error);	
					}							
				}
			}	
		}
	}	
}
