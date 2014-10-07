/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.plista.kornakapi.core.optimizer;

import java.io.File;
import java.io.IOException;
 
import org.plista.kornakapi.web.Components;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptimizeRecommenderJob implements Job{
	
	private static final Logger log = LoggerFactory.getLogger(OptimizeRecommenderJob.class);	
	public static final String RECOMMENDER_NAME_PARAM = OptimizeRecommenderJob.class.getName() + ".optimizerName";

	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {

	    Components components = Components.instance();

	    String recommenderName = context.getJobDetail().getJobDataMap().getString(RECOMMENDER_NAME_PARAM);
	    String label = recommenderName.substring(recommenderName.indexOf("_")+1);
	    Optimizer opt = new FactorizationBasedInMemoryOptimizer();

	    log.info("Training for recommender [{}] started.", recommenderName);
	    try {
	      opt.optimize(new File(components.getConfiguration().getFactorizationbasedOptimizer().getOptimizationDirectory()), components.getConfiguration(), components.getConfiguration().getNumProcessorsForTraining(), label);
	    } catch (IOException e) {
	      log.warn("Optimization of recommender [" + recommenderName + "] failed!", e);
	    }
	    log.info("Training for recommender [{}] done.", recommenderName);
		
	}

}
