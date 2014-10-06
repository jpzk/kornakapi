package org.plista.kornakapi.core.training;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.clustering.lda.cvb.TopicModel;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SparseRowMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.plista.kornakapi.core.config.LDARecommenderConfig;
import org.plista.kornakapi.core.config.RecommenderConfig;

import com.google.common.io.Closeables;

public class DocumentTopicInferenceTrainer extends AbstractTrainer{
	private LDARecommenderConfig conf;
	org.apache.hadoop.conf.Configuration lconf = new org.apache.hadoop.conf.Configuration(); 
	private FileSystem fs;
	private int modelWeight = 1;
	private HashMap<String,Integer> dict;
	private Path path;
	private HashMap<Integer,String> indexItem;
	private HashMap<String,Integer> itemIndex;
	private HashMap<String, Vector> itemFeatures;
	private String itemid;
	

	public DocumentTopicInferenceTrainer(RecommenderConfig conf, Path path, String itemid) {
		super(conf);
		this.conf = (LDARecommenderConfig)conf;
		try {
			fs = FileSystem.get(lconf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.dict = this.getDict();
		this.path = path;
		this.itemid = itemid;
	}
	
	
	/**
	 * 
	 * @param itemid
	 */
	private void inferTopics(int TrainingThreats, String itemid){      
		 Vector item;
		try {
			item = createVectorFromFile(itemid);		 
			TopicModel model = new TopicModel(lconf, conf.getEta(), conf.getAlpha(), getDictAsArray(), TrainingThreats, modelWeight, 
					new Path(this.conf.getTopicsOutputPath())); 
			 Vector docTopics = new DenseVector(new double[model.getNumTopics()]).assign(1.0/model.getNumTopics());
			 Matrix docTopicModel = new SparseRowMatrix(model.getNumTopics(), item.size());
			 int maxIters = 5000;
		        for(int i = 0; i < maxIters; i++) {
		            model.trainDocTopicModel(item, docTopics, docTopicModel);
		        }
		    model.stop();
		    itemFeatures.put(itemid, docTopics);
		    indexItem.put(indexItem.size()+1, itemid);
		    itemIndex.put(itemid, itemIndex.size() +1);
		    
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	/**
	 * 
	 * @return
	 * @throws IOException
	 */
	private String[] getDictAsArray() throws IOException{
		ArrayList<String> dict = new ArrayList<String>();
        Reader reader = new SequenceFile.Reader(fs,new Path(this.conf.getTopicsDictionaryPath()) , lconf);
        Text keyNewDict = new Text();
        IntWritable newVal = new IntWritable();
        while(reader.next(keyNewDict,newVal)){
            dict.add(keyNewDict.toString());
        }
		Closeables.close(reader, false);
        return dict.toArray(new String[dict.size()]);
	}
	/**
	 * 
	 * @param itemid
	 * @return
	 * @throws Exception
	 */
	private Vector createVectorFromFile(String itemid) throws Exception{
		FromFileVectorizer vectorizer = new FromFileVectorizer(conf, itemid);
		vectorizer.doTrain();
		ArrayList<String> newDict = getFileDict(itemid);
		
				
        HashMap<String, Long> newDocTermFreq = new HashMap<String, Long>();
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(conf.getInferencePath() + "/sparsein/wordcount/part-r-00000"), lconf);
        Text key = new Text();
        LongWritable val = new LongWritable();
        while(reader.next(key, val)){
            newDocTermFreq.put(key.toString(), Long.parseLong(val.toString()));
        }
		RandomAccessSparseVector newDocVector = new RandomAccessSparseVector(dict.size());
        for (String string : newDict) {
            if(dict.containsKey(string)){
                int index = dict.get(string);
                double tf = newDocTermFreq.get(string);
                newDocVector.set(index, tf);
            }
        }
        Closeables.close(reader, false);
        return newDocVector;
	}
	/**
	 * 
	 * @return
	 */
	private HashMap<String,Integer> getDict(){
        HashMap<String,Integer> modelDictionary = new HashMap<String, Integer>();
		try {	
			Reader reader = new SequenceFile.Reader(fs,new Path(this.conf.getTopicsDictionaryPath()) , lconf);
			Text keyModelDict = new Text();
		    IntWritable valModelDict = new IntWritable();
		    while(reader.next(keyModelDict, valModelDict)){
		        modelDictionary.put(keyModelDict.toString(), Integer.parseInt(valModelDict.toString()));
		    }   
			Closeables.close(reader, false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return modelDictionary;

	} 
	
	/*
	 * 
	 */
	private ArrayList<String> getFileDict(String itemid) throws IOException{
        ArrayList<String> newDocDictionaryWords = new ArrayList<String>();
        Reader reader = new SequenceFile.Reader(fs, new Path(conf.getInferencePath() + "/sparsein/dictionary.file-0"), lconf);
        Text keyNewDict = new Text();
        IntWritable newVal = new IntWritable();
        while(reader.next(keyNewDict,newVal)){
            newDocDictionaryWords.add(keyNewDict.toString());
        }
        Closeables.close(reader, false);
        return newDocDictionaryWords;
	}
	
	/**
	 * Method to safe the model
	 * @throws IOException
	 */
		public void safe() throws IOException{
			if(itemFeatures !=null){
				Path model = path.suffix("/itemFeature.model");
				Writer w = SequenceFile.createWriter(fs,lconf,model, Text.class, VectorWritable.class);
				for(String itemid : itemFeatures.keySet()){
					Text id = new Text();
					VectorWritable val = new VectorWritable();
					id.set(itemid);
					val.set(itemFeatures.get(itemid));
					w.append(id, val);
				}
				Closeables.close(w, false);
			}
			if(indexItem != null){
				Path model = path.suffix("/indexItem.model");
				Writer w = SequenceFile.createWriter(fs,lconf,model, IntWritable.class, Text.class);
				for(Integer itemid : indexItem.keySet()){
					IntWritable key = new IntWritable();
					Text val = new Text();
					key.set(itemid);
					val.set(indexItem.get(itemid));
					w.append(key, val);
				}
				Closeables.close(w, false);
			}
			if(itemIndex !=null){
				Path model = path.suffix("/itemIndex.model");
				Writer w = SequenceFile.createWriter(fs,lconf,model, Text.class, IntWritable.class);
				for(String itemid : itemIndex.keySet()){
					IntWritable val= new IntWritable();
					Text key  = new Text();
					key.set(itemid);
					val.set(itemIndex.get(itemid));
					w.append(key, val);
				}
				Closeables.close(w, false);
			}
		}
		
		/**
		 * method to load model from squence file
		 * @throws IOException
		 */
		public void read() throws IOException{
			Path indexPath = path.suffix("/indexItem.model");
			if(fs.exists(indexPath)){
				indexItem = new HashMap<Integer,String>();
				Reader reader = new SequenceFile.Reader(fs, indexPath , lconf);
				IntWritable key = new IntWritable();
				Text val = new Text();
				while(reader.next(key,val)){
					indexItem.put(key.get(), val.toString());
				}
				Closeables.close(reader, false);
			}
			
			Path itemIndexPath = path.suffix("/itemIndex.model");
			if(fs.exists(itemIndexPath)){
				itemIndex = new HashMap<String,Integer>();
				Reader reader = new SequenceFile.Reader(fs, itemIndexPath , lconf);
				IntWritable val = new IntWritable();
				Text key= new Text();
				while(reader.next(key,val)){
					itemIndex.put(key.toString(), val.get());
				}
				Closeables.close(reader, false);
			}
			
			Path featurePath = path.suffix("/itemFeature.model"); 
			if(fs.exists(featurePath)){
				Reader reader = new SequenceFile.Reader(fs, featurePath , lconf);
				itemFeatures = new HashMap<String,Vector>();
				Text key = new Text();
				VectorWritable val = new VectorWritable();
				while(reader.next(key,val)){
					itemFeatures.put(key.toString(), val.get());
				}
				Closeables.close(reader, false);
			}
		}


	@Override
	protected void doTrain(File targetFile, DataModel inmemoryData,
			int numProcessors) throws IOException {
		read();
		inferTopics(conf.getInferenceThreats(), itemid);
		safe();
		
	}

}
