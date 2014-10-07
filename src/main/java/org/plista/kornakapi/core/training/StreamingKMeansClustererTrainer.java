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
import org.plista.kornakapi.core.cluster.StreamingKMeansClassifierModel;
import org.plista.kornakapi.core.config.StreamingKMeansClustererConfig;
public class StreamingKMeansClustererTrainer extends AbstractTrainer{


	StramingKMeansClusterer clusterer;
	long start;
	private boolean firstTraining = true;
	private long clusterTimeWindow;
	

	public StreamingKMeansClustererTrainer(StreamingKMeansClustererConfig conf, StreamingKMeansClassifierModel model) throws IOException {
		super(conf);
		clusterer = new StramingKMeansClusterer(model, conf.getDesiredNumCluster(), conf.getDistanceCutoff());
		clusterTimeWindow = conf.getClusterTimeWindow();
	    start = System.currentTimeMillis();
		

	}

	@Override
	protected void doTrain(File targetFile, DataModel inmemoryData,
			int numProcessors) throws IOException {

		if(start - System.currentTimeMillis() > clusterTimeWindow || firstTraining){ //if 12 hours passed retrain hole model
			clusterer.cluster();
			start = System.currentTimeMillis();
			firstTraining = false;
		}else{
			clusterer.stream();
		}
				
	}

}

