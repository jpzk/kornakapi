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

import java.util.List;

import org.apache.mahout.clustering.streaming.cluster.StreamingKMeans;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.math.Centroid;
import org.apache.mahout.math.neighborhood.FastProjectionSearch;
import org.apache.mahout.math.neighborhood.UpdatableSearcher;
import org.plista.kornakapi.core.cluster.StreamingKMeansClassifierModel;
import org.plista.kornakapi.core.storage.MySqlKMeansDataFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StramingKMeansClusterer {
	MySqlKMeansDataFilter extractor;
	StreamingKMeansClassifierModel model;
	int clusters;
	long cutoff;
	StreamingKMeans clusterer;
	
	private static final Logger log = LoggerFactory.getLogger(StramingKMeansClusterer.class);
	
	public StramingKMeansClusterer(StreamingKMeansClassifierModel model,int clusters,long cutoff){
		this.model = model;
		this.clusters = clusters;
		this.cutoff = cutoff;
		UpdatableSearcher searcher = new FastProjectionSearch(new ManhattanDistanceMeasure(), 10, 10);	
		clusterer = new StreamingKMeans(searcher, clusters,cutoff);
	}
	/**
	 * retrain model
	 */
	public void cluster(){
	    long start = System.currentTimeMillis();
	    List<Centroid> data = model.getData();
		UpdatableSearcher centroids = clusterer.cluster(data);
		long estimateDuration = System.currentTimeMillis() - start;  
		this.model.updateCentroids(centroids);
		if (log.isInfoEnabled()) {
			log.info("Model trained in {} ms, created [{}] Clusters", estimateDuration, centroids.size());
		}
	}
	/**
	 * just stream new available data-points into old coordinate system
	 */
	public void stream(){
	    long start = System.currentTimeMillis();
		List<Centroid> data = model.getNewData();
		UpdatableSearcher centroids = clusterer.cluster(data);
		long estimateDuration = System.currentTimeMillis() - start;  
		this.model.updateCentroids(centroids);	
		if (log.isInfoEnabled()) {
			log.info("Model trained in {} ms, created [{}] Clusters", estimateDuration, centroids.size());
		}
	}
}
