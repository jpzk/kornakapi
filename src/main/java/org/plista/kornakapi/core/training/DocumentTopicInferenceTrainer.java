
package org.plista.kornakapi.core.training;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import com.google.common.io.Files;


public class DocumentTopicInferenceTrainer extends AbstractTrainer{
	private LDARecommenderConfig conf;
	org.apache.hadoop.conf.Configuration lconf = new org.apache.hadoop.conf.Configuration(); 
	private FileSystem fs;
	private int modelWeight = 1;

	private Path path;
	private HashMap<Integer,String> indexItem;
	private HashMap<String,Integer> itemIndex;
	private HashMap<String, Vector> itemFeatures;
	private int trainingThreads;
	

	public DocumentTopicInferenceTrainer(RecommenderConfig conf, Path path) {
		super(conf);
		this.conf = (LDARecommenderConfig)conf;
		try {
			fs = FileSystem.get(lconf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		this.path = path;
		trainingThreads = this.conf.getInferenceThreats();
	}
	
	
	/**
	 * 
	 * @param itemid
	 */
	private void inferTopics(String itemid, Vector item){      
		if(itemFeatures.containsKey(itemid)){
			return;
		}
		try {			
			TopicModel model = new TopicModel(lconf, conf.getEta(), conf.getAlpha(), getDictAsArray(), trainingThreads, modelWeight, 
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
	 */
	private void inferTopicsForItems(){
		HashMap<String, Vector> tfVectors = createVectorsFromDir();
		if(tfVectors== null){ //If there are now topics then there is nothing to infere
			return;
		}
		for(String itemid : tfVectors.keySet()){
			inferTopics(itemid, tfVectors.get(itemid));
		}
		try {
			safe();
		} catch (IOException e) {
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

	
	private HashMap<String,Vector> getNewVectors(){
		HashMap<String, Vector> newVectors = new HashMap<String, Vector>();
		try {
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(conf.getInferencePath() + "sparsein/tf-vectors/part-r-00000"), lconf);
			Text key = new Text();
			VectorWritable val = new VectorWritable();
			while(reader.next(key, val)){
				newVectors.put(key.toString().substring(1), val.get());
			}
			Closeables.close(reader, false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return newVectors;
	}
	/**
	 * @throws IOException 
	 * 
	 */
	private HashMap<String, Vector> createVectorsFromDir() {
        HashMap<String, Vector> newVectors = getNewVectors();
		cleanup(newVectors);
		ArrayList<String> newDict = null;
		HashMap<String,Integer> oldDict = null;
		try {
			newDict = this.getFileDict();
			oldDict = getDict();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        HashMap<String, Vector> tfVectors = new HashMap<String, Vector>();  
		if(oldDict != null){
	        for(String key : newVectors.keySet()){
	        	RandomAccessSparseVector newArticleTF = new RandomAccessSparseVector(oldDict.size());
	        	Vector val = newVectors.get(key);
	        	for(int i = 0; i< val.size(); i++){
					double tf =val.get(i);
					String ngram = newDict.get(i);
					if(oldDict.containsKey(ngram)){
						int idx = oldDict.get(ngram);
						newArticleTF.set(idx, tf);
					}
	        	}
				tfVectors.put(key.toString(), newArticleTF);	
	        }
		}
		return tfVectors;
	}
	

	/**
	 * 
	 * @return
	 * @throws IOException 
	 */
	private HashMap<String,Integer> getDict() throws IOException{
        HashMap<String,Integer> modelDictionary = new HashMap<String, Integer>();
		Reader reader = new SequenceFile.Reader(fs,new Path(this.conf.getTopicsDictionaryPath()) , lconf);
		Text keyModelDict = new Text();
	    IntWritable valModelDict = new IntWritable();
	    while(reader.next(keyModelDict, valModelDict)){
	        modelDictionary.put(keyModelDict.toString(), Integer.parseInt(valModelDict.toString()));
	    }   
		Closeables.close(reader, false);

    	return modelDictionary;

	} 
	
	/*
	 * 
	 */
	private ArrayList<String> getFileDict() throws IOException{
        ArrayList<String> dict = new ArrayList<String>();
        Reader reader = new SequenceFile.Reader(fs, new Path(conf.getInferencePath() + "/sparsein/dictionary.file-0"), lconf);
        Text keyText = new Text();
        IntWritable valCount = new IntWritable();
        while(reader.next(keyText,valCount)){
            dict.add(keyText.toString());
        }
        Closeables.close(reader, false);
        return dict;
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
		FromFileVectorizer vectorizer = new FromFileVectorizer(conf);
		try {
			vectorizer.doTrain();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		inferTopicsForItems();

	}
	
	/**
	 * Moves new Articles to the Corpus
	 * @param tfVectors
	 */
	protected void cleanup(HashMap<String, Vector> tfVectors){
		for(String key : tfVectors.keySet()){
			File from = new File(conf.getInferencePath() + "Documents/"+ conf.getTrainingSetName() +"/" + key);
			File to = new File(conf.getTextDirectoryPath()+ key);
			try {
				File newFile = new File(to.toString());
				if(newFile.exists()){
					newFile.delete();
				}
				Files.copy(from, to);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
	}
}
