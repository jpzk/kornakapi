package org.plista.kornakapi.core.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.math.Centroid;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.WeightedVector;
import org.apache.mahout.math.neighborhood.UpdatableSearcher;
import org.apache.mahout.math.random.WeightedThing;
import org.plista.kornakapi.core.config.StorageConfiguration;
import org.plista.kornakapi.core.storage.MySqlKMeansDataFilter;
import org.plista.kornakapi.core.storage.MySqlKMeansDataFilter.StreamingKMeansDataObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingKMeansClassifierModel {

	private UpdatableSearcher centroids = null;
	private double maxWeight = 0;
	private double meanVolume=0;
	private HashMap<Long, FastIDSet> userItemIds = null;
	private FastIDSet userids = null;
	private int dim=0;
	private HashMap<Long, WeightedThing<Vector>> itemID2Centroid = new HashMap<Long, WeightedThing<Vector>>();
	private static final Logger log = LoggerFactory.getLogger(StreamingKMeansClassifierModel.class);
	private StorageConfiguration conf;
	private FastIDSet allItems;
	private int initialDim = 300;
	private String label;
	
	public StreamingKMeansClassifierModel(StorageConfiguration conf, String label){
		this.conf = conf;
		this.label = label;
	}
	
	public void setData(StreamingKMeansDataObject data){
		this.userItemIds = data.getUserItemIDs();		
		this.userids = data.getUserIDs();
		this.dim = data.getDim();
		this.allItems = data.getAllItems();
	}
/**
 * Method that updates the model if new centroids are callculated
 * @param data
 * @param centroids
 */
	public void updateCentroids (UpdatableSearcher centroids){		
		this.centroids = centroids;
		if (log.isInfoEnabled()) {
			log.info("Computed "+centroids.size()+ " clusters \n");
		}
		Iterator<Vector> iter =centroids.iterator();
		while(iter.hasNext()){
			Centroid cent = (Centroid) iter.next();
			double weight =cent.getWeight();
			if(weight > maxWeight){
				maxWeight = weight;
			}
		}
		iter =centroids.iterator();
		int i = 0;
		while(iter.hasNext()){		
			Centroid cent = (Centroid) iter.next();
			meanVolume += cent.getWeight()/maxWeight* cent.getNumNonZeroElements();
			i++;
		    if (log.isInfoEnabled()) {
		    	log.info("NormWeight= [{}], l2norm= [{}], Number of Users= [{}] Volume= [{}]",
		    			new Object[] {cent.getWeight()/this.maxWeight, cent.norm(2),cent.getNumNonZeroElements() , (cent.getWeight()/maxWeight)* cent.getNumNonZeroElements() }); 			    			
		    }
		}
		meanVolume = meanVolume/i;
		this.itemID2Centroid.clear();
	}

	public UpdatableSearcher getCentroids(){
		return this.centroids;
	}
	
	public double getMaxWeight(){
		return this.maxWeight;
	}
	
	public double getMeanVolume(){
		return this.meanVolume;
	}
			
	/**
	 * Returns the SequentialAccessSparseVector of an item id
	 * @param itemId
	 * @return RandomAccessSparseVector
	 * @throws IOException 
	 */
	public SequentialAccessSparseVector createVector(long itemId) throws IOException{
		SequentialAccessSparseVector itemVector = new SequentialAccessSparseVector(dim, initialDim);
		int i = 0;
		boolean isRated = false;
 		for(long userid : userids.toArray()){ 			
 			FastIDSet itemIds = userItemIds.get(userid);
 			if(itemIds.contains(itemId)){
 				itemVector.set(i, 1);
 				isRated = true;
 			}
 			i++;
 		}	
 		if(isRated){
 			return itemVector;
 		}else{
 			throw new IOException("Item unknown");
 		}
	}
	
	
	/**
	 * 
	 * @param itemID
	 * @return
	 * @throws IOException
	 */
	public WeightedThing<Vector> getClossestCentroid(long itemID) throws IOException{
		if(itemID2Centroid.containsKey(itemID)){
			return itemID2Centroid.get(itemID);
		}else{
			WeightedThing<Vector> cent = centroids.searchFirst(createVector(itemID), false);
			itemID2Centroid.put(itemID, cent);
			return cent;
		}	
	}
	/**
	 * Gets data in old coordinate system
	 * @return
	 */
	public List<Centroid> getNewData(){
		MySqlKMeansDataFilter extractor = new MySqlKMeansDataFilter(conf, label);
		StreamingKMeansDataObject data = extractor.getNewData(userids, dim);
		try {
			extractor.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		this.userItemIds = data.getUserItemIDs();
		
		/**
		 * new items might only exist in the new userspace/coordinate system
		 * but not in the old one wich is still used in this method and remains unchanged.
		 * Therefore new items might not be concidered here
		 */
		ArrayList<Centroid> itemVectors = new ArrayList<Centroid>();
		if(!this.allItems.equals(data.getAllItems())){
			int i = 0;
			for(Long itemID :data.getAllItems()){ 
				if(!allItems.contains(itemID)){
					try {
						itemVectors.add(new Centroid(new WeightedVector(createVector(itemID), 1,i)));
						i++;
					} catch (IOException e) {
					    if (log.isInfoEnabled()) {
					    	log.info(e.getMessage()) ;			    			
					    }

					}
				}
			}
		}
	    if (log.isInfoEnabled()) {
	    	log.info("Adding [{}] new Items", itemVectors.size()) ;			    			
	    }
		this.allItems = data.getAllItems();
		return itemVectors;
	}
	
	
	/**
	 * Gets data in new coordinate system
	 * @return
	 */
	public ArrayList<Centroid> getData(){
		MySqlKMeansDataFilter extractor = new MySqlKMeansDataFilter(conf, label);
		StreamingKMeansDataObject data = extractor.getData();
		try {
			extractor.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		this.setData(data);

		ArrayList<Centroid> itemVectors = new ArrayList<Centroid>();
		int n = 0;
	 	for(long itemId : allItems.toArray()){
	 		try {
	 			SequentialAccessSparseVector itemVector = createVector(itemId);
				itemVectors.add(new Centroid(new WeightedVector(itemVector, 1,n))); 
				n++;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	 	}
	 	if (log.isInfoEnabled()) {
		 	log.info("Done!");
	 	}
	 	return itemVectors;
	}
}
