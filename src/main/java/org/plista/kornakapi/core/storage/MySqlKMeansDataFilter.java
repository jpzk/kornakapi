package org.plista.kornakapi.core.storage;




import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.common.IOUtils;
import org.plista.kornakapi.core.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




public class MySqlKMeansDataFilter extends MySqlStorage{
	

	private static final String GET_USER = "select user_id from (SELECT user_id, COUNT(user_id) AS nums FROM taste_preferences GROUP BY user_id ORDER BY nums DESC) as ns where nums >";
	private static final String GET_USER_ITEMS_BASE = "SELECT item_id FROM taste_preferences WHERE user_id = ";
	private static final String GET_ALL_RATED_ITEMS = "SELECT DISTINCT(item_id) FROM taste_preferences";
	private static final Logger log = LoggerFactory.getLogger(MySqlKMeansDataFilter.class);
	private int minNumUserRatings;
	//private static int initialCapacity = 2000;
	//private static final String test = "SELECT * FROM taste_preferences";


/**
 * 	
 * @param storageConf
 */
	 public MySqlKMeansDataFilter(StorageConfiguration storageConf, String label){
			super(storageConf, label);
			this.minNumUserRatings = storageConf.getMinNumUserRatings();
	  }
	 /**
	  * 
	  * @return
	  */
	 public StreamingKMeansDataObject getData(){	
		 /**
		  * get all userids according to the top query
		  */
		 	FastIDSet userids = this.getQuery(GET_USER + String.valueOf(minNumUserRatings));
		 	HashMap<Long, FastIDSet> userItemIds = new HashMap<Long, FastIDSet>();
		 	FastIDSet allItems = new FastIDSet();
		 	int dim = userids.size();

		 	
		 	/**
		 	 * for all users: get all items
		 	 */
		 	for(long userid : userids.toArray()){
		 		String getUserItems = GET_USER_ITEMS_BASE + String.valueOf(userid);
		 		FastIDSet userItems = getQuery(getUserItems);
		 		allItems.addAll(userItems);
		 		userItemIds.put(userid, userItems);		 		
		 	}
		 	if (log.isInfoEnabled()) {
			 	int numAllRatedItems = this.getQuery(GET_ALL_RATED_ITEMS).size();
			 	int numAllConcideredItems = allItems.size(); 
			 	log.info("Creating [{}] Vectors with [{}] dimensions out of [{}] items.",
			 			new Object[] {numAllConcideredItems, dim, numAllRatedItems});
		 	}
		return new StreamingKMeansDataObject(allItems, userids, userItemIds, dim );
	 }

	 /**
	  * Data object containing all important variables
	  *
	  */
	 public class StreamingKMeansDataObject{
		private FastIDSet userids;
		private HashMap<Long, FastIDSet> userItemIds;
		private int dim;
		private FastIDSet allItems;

		
		public StreamingKMeansDataObject(FastIDSet allItems, FastIDSet userids, HashMap<Long, FastIDSet> userItemIds, int dim){
			this.allItems = allItems;
			this.userids = userids;
			this.userItemIds = userItemIds;
			this.dim = dim;
		 }
		 
		public FastIDSet getUserIDs(){
			return this.userids;
		}
		public HashMap<Long, FastIDSet> getUserItemIDs(){
			return this.userItemIds;
		}
		 
		public int getDim(){
			return this.dim;
		}		
		public FastIDSet getAllItems(){
			return this.allItems;
		}

	}
	 
	public FastIDSet getQuery(String query){
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		FastIDSet candidates = new FastIDSet();
		 
		try {
		      conn = dataSource.getConnection();
		      stmt = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
		          ResultSet.CONCUR_READ_ONLY);
		      stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
		      stmt.setFetchSize(1000);
		      rs = stmt.executeQuery();
		      while (rs.next()) {
		        candidates.add(rs.getLong(1));
		      }
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
		      IOUtils.quietClose(stmt);
		      IOUtils.quietClose(conn);
		}
		return candidates;
	}
	
	public StreamingKMeansDataObject getNewData(FastIDSet userIDs, int dim){
	 	HashMap<Long, FastIDSet> userItemIds = new HashMap<Long, FastIDSet>();
	 	FastIDSet allItems = new FastIDSet();
	 	for(long userid : userIDs.toArray()){
	 		String getUserItems = GET_USER_ITEMS_BASE + String.valueOf(userid);
	 		FastIDSet userItems = getQuery(getUserItems);
	 		allItems.addAll(userItems);
	 		userItemIds.put(userid, userItems);	 		
	 	}
	 	if (log.isInfoEnabled()) {
		 	int numAllRatedItems = this.getQuery(GET_ALL_RATED_ITEMS).size();
		 	int numAllConcideredItems = allItems.size(); 
		 	log.info("Creating [{}] Vectors with [{}] dimensions out of [{}] items.",
		 			new Object[] {numAllConcideredItems,dim, numAllRatedItems });
	 	}
	 	return new StreamingKMeansDataObject(allItems, userIDs, userItemIds, dim );
	}
}
