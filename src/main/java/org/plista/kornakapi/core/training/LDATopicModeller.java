package org.plista.kornakapi.core.training;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.cf.taste.model.DataModel;
import org.plista.kornakapi.core.config.LDARecommenderConfig;
import org.plista.kornakapi.core.config.RecommenderConfig;



public class LDATopicModeller extends AbstractTrainer{

	
	
	protected RecommenderConfig conf;

	protected LDATopicModeller(RecommenderConfig conf) throws IOException {
		super(conf);
		this.conf = conf;

	}

	protected void doTrain() throws Exception {
		/**			//MapReduce
	       CVB0Driver driver = new CVB0Driver();			
	       Configuration jobConf = new Configuration();
	       driver.run(jobConf, sparseVectorIn.suffix("/matrix"),
	        		topicsOut, k, 2000, doc_topic_smoothening, term_topic_smoothening,
	                maxIter, iteration_block_size, convergenceDelta,
	               new Path(((LDARecommenderConfig)conf).getTopicsDictionaryPath()), new Path(((LDARecommenderConfig)conf).getLDADocTopicsPath()), new Path(((LDARecommenderConfig)conf).getTmpLDAModelPath()),
	               seed, testFraction, numTrainThreads, numUpdateThreads, maxItersPerDoc,
	                numReduceTasks, backfillPerplexity);
	       **/
		//Inmemory
			LDATopicFactorizer factorizer = new LDATopicFactorizer(conf);
			SemanticModel semanticModel = factorizer.factorize();
			semanticModel.safe();
			semanticModel = new SemanticModel(new Path (((LDARecommenderConfig)conf).getLDARecommenderModelPath()));
			semanticModel.read();

		
	}
	@Override
	protected void doTrain(File targetFile, DataModel inmemoryData,
			int numProcessors) throws IOException {	
	}
}
