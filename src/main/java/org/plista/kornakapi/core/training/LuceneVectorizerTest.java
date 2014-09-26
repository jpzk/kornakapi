package org.plista.kornakapi.core.training;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.CandidateItemsStrategy;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.clustering.lda.LDAPrintTopics;
import org.apache.mahout.clustering.lda.cvb.TopicModel;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseRowMatrix;
import org.apache.mahout.math.Vector;
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
		
		
        //Get the new document dictionary file
        ArrayList<String> newDocDictionaryWords = new ArrayList<String>();
        org.apache.hadoop.conf.Configuration lconf = new org.apache.hadoop.conf.Configuration(); 
        FileSystem fs = FileSystem.get(lconf);
        Reader reader = new SequenceFile.Reader(fs,new Path(((LDARecommenderConfig)rconf).getTopicsDictionaryPath()) , lconf);
        Text keyNewDict = new Text();
        IntWritable newVal = new IntWritable();
        while(reader.next(keyNewDict,newVal)){
            System.out.println("Key: "+keyNewDict.toString()+" Val: "+newVal);
            newDocDictionaryWords.add(keyNewDict.toString());
        }
		

		TopicModel model = new TopicModel(lconf, 0.1, 0.1, newDocDictionaryWords.toArray(new String[newDocDictionaryWords.size()]), 1, 1f, 
				new Path(((LDARecommenderConfig)rconf).getTopicsOutputPath())); 
		 Vector docTopics = new DenseVector(new double[model.getNumTopics()]).assign(1.0/model.getNumTopics());
		 Vector newDocVector = new DenseVector(10);
		 newDocVector.set(0, 2.5);
		 newDocVector.set(1, 1.5);
		 newDocVector.set(2, 12.5);
		 newDocVector.set(3, 4.5);
		 newDocVector.set(4, 0);
		 newDocVector.set(5, 2.5);
		 newDocVector.set(6, 1.5);
		 newDocVector.set(7, 12.5);
		 newDocVector.set(8, 4.5);
		 newDocVector.set(9, 0);
		 Matrix docTopicModel = new SparseRowMatrix(model.getNumTopics(), newDocVector.size());
		 
		 int maxIters = 100;
	        for(int i = 0; i < maxIters; i++) {
	            model.trainDocTopicModel(newDocVector, docTopics, docTopicModel);
	        }
	     model.stop();
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
		 itemIDs[0]= 6;
		 List<RecommendedItem> items = recommender.recommendToAnonymous(itemIDs, 12, rescorer);
	     System.out.print(items);


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
