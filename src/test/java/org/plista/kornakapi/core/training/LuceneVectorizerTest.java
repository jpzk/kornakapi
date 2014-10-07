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
import java.util.List;


import org.apache.commons.dbcp.BasicDataSource;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.CandidateItemsStrategy;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.clustering.lda.LDAPrintTopics;
import org.apache.mahout.utils.vectors.VectorDumper;
import org.plista.kornakapi.core.config.Configuration;
import org.plista.kornakapi.core.config.LDARecommenderConfig;
import org.plista.kornakapi.core.config.RecommenderConfig;
import org.plista.kornakapi.core.recommender.CachingAllUnknownItemsCandidateItemsStrategy;
import org.plista.kornakapi.core.recommender.FixedCandidatesIDRescorer;
import org.plista.kornakapi.core.recommender.LDATopicRecommender;
import org.plista.kornakapi.core.storage.CandidateCacheStorageDecorator;
import org.plista.kornakapi.core.storage.SemanticMySqlStorage;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class LuceneVectorizerTest {
		
	public static void main(String [] arg) throws Exception{
		String path = "kornakapi.conf";
		File configFile = new File(path);
		System.out.print(configFile.canRead());
		Configuration conf =Configuration.fromXML(Files.toString(configFile, Charsets.UTF_8));		
		RecommenderConfig rconf = conf.getLDARecommender();
		FromDirectoryVectorizer rizer = new FromDirectoryVectorizer(rconf);
		rizer.doTrain();
		LDATopicModeller modeller = new LDATopicModeller(rconf);
		modeller.doTrain();
		//printTopicWordDistribution(rconf,((LDARecommenderConfig)rconf).getTopicsOutputPath(),((LDARecommenderConfig)rconf).getTopicsOutputPath());
		//printDocumentTopicDistribution(rconf,((LDARecommenderConfig)rconf).getLDADocTopicsPath(),((LDARecommenderConfig)rconf).getLDADocTopicsPath());
		//printLDAPrint(rconf,((LDARecommenderConfig)rconf).getLDADocTopicsPath(),((LDARecommenderConfig)rconf).getLDADocTopicsPath());
		printLocalTopicWordDistribution(rconf,((LDARecommenderConfig)rconf).getTopicsOutputPath(),((LDARecommenderConfig)rconf).getTopicsOutputPath());
		printLocalDocumentTopicDistribution(rconf,((LDARecommenderConfig)rconf).getLDADocTopicsPath(),((LDARecommenderConfig)rconf).getLDADocTopicsPath());
		
		

	     BasicDataSource dataSource = new BasicDataSource();
	     String label = "123235";
	     CandidateCacheStorageDecorator dec =new CandidateCacheStorageDecorator(new SemanticMySqlStorage(conf.getStorageConfiguration(), label,dataSource)); 
	     DataModel dmodel = dec.recommenderData();
	     CandidateItemsStrategy allUnknownItemsStrategy =
		            new CachingAllUnknownItemsCandidateItemsStrategy(dmodel);
	     LDATopicRecommender recommender = new LDATopicRecommender(dmodel, allUnknownItemsStrategy , (LDARecommenderConfig)rconf);
	 	 FastIDSet candidates = dec.getCandidates(label);
		 FixedCandidatesIDRescorer rescorer = new FixedCandidatesIDRescorer(candidates);
		 long[] itemIDs = new long[1];
		 itemIDs[0]= 9;
		 List<RecommendedItem> items1 = recommender.recommendToAnonymous(itemIDs, 12, rescorer);
	     System.out.print(items1);
	     
		 itemIDs = new long[1];
		 itemIDs[0]= 10;
		 List<RecommendedItem> items2 = recommender.recommendToAnonymous(itemIDs, 12, rescorer);
	     System.out.print(items1);
	     System.out.print("\n");
	     System.out.print(items2);
	     
	     itemIDs = new long[1];
		 itemIDs[0]= 10;
		 List<RecommendedItem> items3 = recommender.recommendToAnonymous(itemIDs, 12, rescorer);
	     System.out.print("\n");
	     System.out.print(items3);
	     dec.close();

	}
	
	public static void printTopicWordDistribution(RecommenderConfig conf, String input, String output){
	       List<String> argList = Lists.newLinkedList();
	        argList.add("-i");
	        argList.add(input);
	        argList.add("-o");
	        argList.add(output + "/topics.txt");
	        argList.add("--dictionaryType");
	        argList.add("sequencefile");
	        argList.add("-d");
	        argList.add(((LDARecommenderConfig)conf).getTopicsDictionaryPath());
	        argList.add("-sort");
	        argList.add("true");
	        argList.add("-vs");
	        argList.add("20");

	        String[] args = argList.toArray(new String[argList.size()]);
	        try {
				//LDAPrintTopics.main(args);
				VectorDumper.main(args);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	}
	
	
	public static void printLocalTopicWordDistribution(RecommenderConfig conf, String input, String output){
	       List<String> argList = Lists.newLinkedList();
	        argList.add("-i");
	        argList.add(input);
	        argList.add("-o");
	        argList.add("/opt/kornakapi-model/lda/print/topics.txt");
	        argList.add("--dictionaryType");
	        argList.add("sequencefile");
	        argList.add("-d");
	        argList.add(((LDARecommenderConfig)conf).getTopicsDictionaryPath());
	        argList.add("-sort");
	        argList.add("true");
	        argList.add("-vs");
	        argList.add("20");

	        String[] args = argList.toArray(new String[argList.size()]);
	        try {
				//LDAPrintTopics.main(args);
				VectorDumper.main(args);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	}
	
	
	public static void printDocumentTopicDistribution(RecommenderConfig conf, String input, String output){
	       List<String> argList = Lists.newLinkedList();
	        argList.add("-i");
	        argList.add(input);
	        argList.add("-o");
	        argList.add(output + "/DocumentTopics.txt");
	        argList.add("-sort");
	        argList.add("true");
	        argList.add("-vs");
	        argList.add("20");
	        argList.add("-p");
	        argList.add("true");


	        String[] args = argList.toArray(new String[argList.size()]);
	        try {
				//LDAPrintTopics.main(args);
				VectorDumper.main(args);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	}
	
	public static void printLocalDocumentTopicDistribution(RecommenderConfig conf, String input, String output){
	       List<String> argList = Lists.newLinkedList();
	        argList.add("-i");
	        argList.add(input);
	        argList.add("-o");
	        argList.add("/opt/kornakapi-model/lda/print/DocumentTopics.txt");
	        argList.add("-sort");
	        argList.add("true");
	        argList.add("-vs");
	        argList.add("20");
	        argList.add("-p");
	        argList.add("true");


	        String[] args = argList.toArray(new String[argList.size()]);
	        try {
				//LDAPrintTopics.main(args);
				VectorDumper.main(args);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	}
	
	public static void printLDAPrint(RecommenderConfig conf, String input, String output){
	       List<String> argList = Lists.newLinkedList();
	        argList.add("-i");
	        argList.add(input);
	        argList.add("-o");
	        argList.add(output + "/PrintLDA.txt");
	        argList.add("-w");
	        argList.add("1000");
	        argList.add("--dictionaryType");
	        argList.add("sequencefile");
	        argList.add("--dict");
	        argList.add(((LDARecommenderConfig)conf).getTopicsDictionaryPath());
	        



	        String[] args = argList.toArray(new String[argList.size()]);
	        try {
				LDAPrintTopics.main(args);
				//VectorDumper.main(args);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	}
}
