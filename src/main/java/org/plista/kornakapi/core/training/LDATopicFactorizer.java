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

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.clustering.lda.cvb.InMemoryCollapsedVariationalBayes0;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.plista.kornakapi.core.config.LDARecommenderConfig;
import org.plista.kornakapi.core.config.RecommenderConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;


/**
 * 
 *
 */
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
    private LDARecommenderConfig conf;
	private Integer k;
	private Double alpha;
	private Double eta;
	private Double convergenceDelta = 0.0;
	org.apache.hadoop.conf.Configuration lconf = new org.apache.hadoop.conf.Configuration(); 
	FileSystem fs;
	private HashMap<Integer,String> indexItem = null;
	private HashMap<String,Vector> itemFeatures;
	private HashMap<String,Integer> itemIndex = null;
	
	/**
	 * 
	 * @param conf
	 * @throws TasteException
	 * @throws IOException
	 */
	protected LDATopicFactorizer(RecommenderConfig conf) throws TasteException, IOException {
		this.conf = (LDARecommenderConfig)conf;
		sparseVectorIn= new Path(this.conf.getCVBInputPath());
		topicsOut= new Path(this.conf.getTopicsOutputPath());
		k= new Integer(this.conf.getnumberOfTopics());
        fs = FileSystem.get(lconf);
        this.alpha = this.conf.getAlpha();
        this.eta = this.conf.getEta();
	}
	
	/**
	 * 
	 * @throws IOException
	 */
	private void indexItem() throws IOException{
		if(indexItem == null){
			indexItem = new HashMap<Integer,String>();
			itemIndex = new HashMap<String,Integer>();
			Reader reader = new SequenceFile.Reader(fs,new Path(this.conf.getCVBInputPath() + "/docIndex") , lconf);
			IntWritable key= new IntWritable();
			Text  newVal = new Text();
			while(reader.next(key, newVal)){
				indexItem.put(key.get(),newVal.toString().substring(1));
				itemIndex.put(newVal.toString().substring(1),key.get());
			}
			Closeables.close(reader, false);
		}
	}
	
	/**
	 * 
	 * @param itemid
	 * @return
	 * @throws IOException
	 */
	public Integer getitemIndex(String itemid) throws IOException{
		if(itemIndex==null){
			indexItem();
		}
		return itemIndex.get(itemid);
	}
	
	/**
	 * 
	 * @param idx
	 * @return
	 * @throws IOException
	 */
	public String getIndexItem(Integer idx) throws IOException{
		if(indexItem==null){
			indexItem();
		}
		return indexItem.get(idx);
	}
	

	
	/**
	 * gets topic posterior from lda output
	 * @throws IOException
	 */
	private void getAllTopicPosterior() throws IOException{
		itemFeatures= new HashMap<String,Vector>();
		Reader reader = new SequenceFile.Reader(fs,new Path(this.conf.getLDADocTopicsPath()) , lconf);
		IntWritable key = new IntWritable();
		VectorWritable newVal = new VectorWritable();
		while(reader.next(key, newVal)){
			itemFeatures.put(getIndexItem(key.get()), newVal.get());
		}
		Closeables.close(reader, false);		
	}
	
	/**
	 * 
	 * @return
	 * @throws TasteException
	 */
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
        argList.add("-c");
        argList.add(convergenceDelta.toString());
        argList.add("-ntt");
        argList.add(((LDARecommenderConfig)conf).getTrainingThreats().toString());
        argList.add("-m");
        argList.add("30");
       String[] args = argList.toArray(new String[argList.size()]);
       try {
		InMemoryCollapsedVariationalBayes0.main(args);
	    //computeAllTopicPosterior();
		getAllTopicPosterior();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       return  new SemanticModel(indexItem,itemIndex, itemFeatures, new Path(((LDARecommenderConfig)conf).getLDARecommenderModelPath()),conf);
	}
}
