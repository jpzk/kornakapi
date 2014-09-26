package org.plista.kornakapi.core.training;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.cf.taste.common.NoSuchItemException;

import com.google.common.io.Closeables;

public class SemanticModel{
	private HashMap<Integer,String> indexItem;
	private HashMap<String,Integer> itemIndex;
	private HashMap<String, Vector> itemFeatures;
	private Path path;
	org.apache.hadoop.conf.Configuration lconf = new org.apache.hadoop.conf.Configuration(); 
	FileSystem fs;
	
	public SemanticModel(HashMap<Integer,String> indexItem, HashMap<String,Integer> itemIndex, HashMap<String, Vector> itemFeatures, Path path){
		this.indexItem = indexItem;
		this.itemIndex = itemIndex;
		this.itemFeatures = itemFeatures;
		this.path = path;
		try {
			fs = FileSystem.get(lconf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public SemanticModel(Path path){
		this.path = path;
		try {
			fs = FileSystem.get(lconf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public HashMap<Integer,String> getIndexItem(){
		return indexItem;
	}
	public HashMap<String,Integer> getItemIndex(){
		return itemIndex;
	}
	public Integer getItemIndex(String itemid){
		return itemIndex.get(itemid);
	}
	public  HashMap<String, Vector> getItemFeatures(){
		return itemFeatures;
	}
	public Vector getItemFeatures(String itemid) throws NoSuchItemException{
		if(itemFeatures.containsKey(itemid)){
			return itemFeatures.get(itemid);
		} else{
			throw new NoSuchItemException("Itemid doesn't not exist");
		}
	}
	
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
}