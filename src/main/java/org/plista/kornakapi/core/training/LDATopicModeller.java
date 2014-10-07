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

import java.io.File;
import java.io.IOException;


import org.apache.mahout.cf.taste.model.DataModel;

import org.plista.kornakapi.core.config.RecommenderConfig;


/**
 * Trainer to train a semantic model using lda
 *
 */
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

		
	}
	@Override
	protected void doTrain(File targetFile, DataModel inmemoryData,
			int numProcessors) throws IOException {	
	}
}
