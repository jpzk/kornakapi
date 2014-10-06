package org.plista.kornakapi.core.training;

import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.model.DataModel;
import org.plista.kornakapi.core.config.RecommenderConfig;

public class LDATrainer extends AbstractTrainer{
	
	private RecommenderConfig conf;
	
	public LDATrainer(RecommenderConfig conf){
		super(conf);
		this.conf = conf;
	}

	@Override
	protected void doTrain(File targetFile, DataModel inmemoryData,
			int numProcessors) throws IOException {
		try {
			new FromDirectoryVectorizer(conf).doTrain();
			new LDATopicModeller(conf).doTrain();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
