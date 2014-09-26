package org.plista.kornakapi.core.training;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.clustering.lda.cvb.InMemoryCollapsedVariationalBayes0;
import org.apache.mahout.clustering.lda.cvb.TopicModel;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseRowMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.plista.kornakapi.core.config.LDARecommenderConfig;
import org.plista.kornakapi.core.config.RecommenderConfig;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

public class LDATopicFactorizer{
	
	/**
    private double doc_topic_smoothening = 0.0001;
    private double term_topic_smoothening = 0.0001;
    private int maxIter = 5;
    private int iteration_block_size = 10;
    private double convergenceDelta = 0;
    private float testFraction = 0.5f;
    private int numTrainThreads = 4;
    private int numUpdateThreads = 1;
    private int maxItersPerDoc = 10;
    private int numReduceTasks = 10;
    private boolean backfillPerplexity = false;
    private int seed = (int) Math.random() * 3021;
	**/
	
    private Path sparseVectorIn;
	private Path topicsOut;
    private RecommenderConfig conf;
	private Integer k;
	private Double alpha = 0.1;
	private Double eta = 0.1;
	private float modelWeight = 1;
	private int trainingThreats = 1; 
	org.apache.hadoop.conf.Configuration lconf = new org.apache.hadoop.conf.Configuration(); 
	FileSystem fs;
	private HashMap<Integer,String> indexItem = null;
	private HashMap<String,Vector> itemFeatures;
	private HashMap<String,Integer> itemIndex = null;
	
	
	protected LDATopicFactorizer(RecommenderConfig conf) throws TasteException, IOException {
		sparseVectorIn= new Path(((LDARecommenderConfig)conf).getCVBInputPath());
		topicsOut= new Path(((LDARecommenderConfig)conf).getTopicsOutputPath());
		k= new Integer(((LDARecommenderConfig)conf).getnumberOfTopics());
		this.conf = conf;
        fs = FileSystem.get(lconf);
	}

	protected String[] getDictAsArray() throws IOException{
		ArrayList<String> dict = new ArrayList<String>();
        Reader reader = new SequenceFile.Reader(fs,new Path(((LDARecommenderConfig)conf).getTopicsDictionaryPath()) , lconf);
        Text keyNewDict = new Text();
        IntWritable newVal = new IntWritable();
        while(reader.next(keyNewDict,newVal)){
            dict.add(keyNewDict.toString());
        }
		Closeables.close(reader, false);
        return dict.toArray(new String[dict.size()]);
	}
	
	protected HashMap<String, Vector> getAllItemVectors() throws IOException{
		HashMap<String,Vector> itemVectors = new HashMap<String,Vector>();
		Reader reader = new SequenceFile.Reader(fs,new Path(((LDARecommenderConfig)conf).getCVBInputPath() + "/matrix") , lconf);
		IntWritable key = new IntWritable();
		VectorWritable newVal = new VectorWritable();
		while(reader.next(key, newVal)){
			itemVectors.put(getIndexItem(key.get()), newVal.get());
		}
		Closeables.close(reader, false);
		return itemVectors;
		
	}
	protected void indexItem() throws IOException{
		if(indexItem == null){
			indexItem = new HashMap<Integer,String>();
			itemIndex = new HashMap<String,Integer>();
			Reader reader = new SequenceFile.Reader(fs,new Path(((LDARecommenderConfig)conf).getCVBInputPath() + "/docIndex") , lconf);
			IntWritable key= new IntWritable();
			Text  newVal = new Text();
			while(reader.next(key, newVal)){
				indexItem.put(key.get(),newVal.toString().substring(1));
				itemIndex.put(newVal.toString().substring(1),key.get());
			}
			Closeables.close(reader, false);
		}
	}
	public Integer getitemIndex(String itemid) throws IOException{
		if(itemIndex==null){
			indexItem();
		}
		return itemIndex.get(itemid);
	}
	public String getIndexItem(Integer idx) throws IOException{
		if(indexItem==null){
			indexItem();
		}
		return indexItem.get(idx);
	}
	
	protected void computeAllTopicPosterior() throws IOException{        
		itemFeatures= new HashMap<String,Vector>();
		TopicModel model = new TopicModel(lconf, eta, alpha, getDictAsArray(), trainingThreats, modelWeight, 
				new Path(((LDARecommenderConfig)conf).getTopicsOutputPath())); 
		 HashMap<String,Vector> allItems = getAllItemVectors();
		 for(String itemid: allItems.keySet()){
			 Vector item = allItems.get(itemid);
			 Vector docTopics = new DenseVector(new double[model.getNumTopics()]).assign(1.0/model.getNumTopics());
			 Matrix docTopicModel = new SparseRowMatrix(model.getNumTopics(), item.size());
			 int maxIters = 100;
		        for(int i = 0; i < maxIters; i++) {
		            model.trainDocTopicModel(item, docTopics, docTopicModel);
		        }
		    model.stop();
		    itemFeatures.put(itemid, docTopics);			 
		 }
	}

	public SemanticModel factorize() throws TasteException {
       	List<String> argList = Lists.newLinkedList();
        argList.add("-i");
        argList.add(sparseVectorIn.toString()+ "/matrix");
        argList.add("-to");
        argList.add(topicsOut.toString() );
        argList.add("--numTopics");
        argList.add(k.toString());
        argList.add("-d");
        argList.add(((LDARecommenderConfig)conf).getTopicsDictionaryPath());
        argList.add("--alpha");
        argList.add(alpha.toString());
        argList.add("--eta");
        argList.add(eta.toString());
        argList.add("-do");
        argList.add(((LDARecommenderConfig)conf).getLDADocTopicsPath());
       String[] args = argList.toArray(new String[argList.size()]);
       try {
		InMemoryCollapsedVariationalBayes0.main(args);
	    computeAllTopicPosterior();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       return  new SemanticModel(indexItem,itemIndex, itemFeatures, new Path(((LDARecommenderConfig)conf).getLDARecommenderModelPath()));
	}
	

}
