package org.plista.kornakapi.core.recommender;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import com.google.common.collect.Lists;

import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.recommender.ByValueRecommendedItemComparator;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.impl.recommender.SimilarUser;
import org.apache.mahout.cf.taste.impl.recommender.TopItems.Estimator;
import org.apache.mahout.cf.taste.impl.similarity.GenericItemSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.GenericUserSimilarity;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

import com.google.common.base.Preconditions;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

/**
 * This is somewhat hacky, but it might work. 
 * 
 * @author jendrik.poloczek
 *
 */
public final class CustomTopItems {
  
  private static final long[] NO_IDS = new long[0];
  private Connection connection;
  
  public CustomTopItems() throws SQLException { 
	
		    this.connection = (Connection) DriverManager.getConnection(
		    		"jdbc:mysql://plista249.plista.com/db_youfilter?" +
                "creds");

  };
  
  public List<RecommendedItem> getTopItems(int howMany,
                                                  LongPrimitiveIterator possibleItemIDs,
                                                  IDRescorer rescorer,
                                                  Estimator<Long> estimator) throws TasteException, SQLException {
    Preconditions.checkArgument(possibleItemIDs != null, "possibleItemIDs is null");
    Preconditions.checkArgument(estimator != null, "estimator is null");

    Queue<RecommendedItem> topItems = new PriorityQueue<RecommendedItem>(howMany + 1,
      Collections.reverseOrder(ByValueRecommendedItemComparator.getInstance()));
    boolean full = false;
    double lowestTopValue = Double.NEGATIVE_INFINITY;
    while (possibleItemIDs.hasNext()) {
      long itemID = possibleItemIDs.next();
      if (rescorer == null || !rescorer.isFiltered(itemID)) {
        double preference;
        try {
          preference = estimator.estimate(itemID);
        } catch (NoSuchItemException nsie) {
          continue;
        }
        double rescoredPref = rescorer == null ? preference : rescorer.rescore(itemID, preference);
        if (!Double.isNaN(rescoredPref) && (!full || rescoredPref > lowestTopValue)) {
        	
        	// Only add the item to the top items if it is newer than 14 days. 
		    Statement stmt = (Statement) this.connection.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT COUNT(itemid) AS c from item WHERE itemid = " + itemID + 
		    		" and created_at > DATE_SUB(NOW(), INTERVAL 14 DAY)");
		    rs.next();
		    int c = rs.getInt("c");
		    if(c > 0) {
		    	topItems.add(new GenericRecommendedItem(itemID, (float) rescoredPref));
		    } else {
		    	continue;
		    }
		    
          if (full) {
            topItems.poll();
          } else if (topItems.size() > howMany) {
            full = true;
            topItems.poll();
          }
          lowestTopValue = topItems.peek().getValue();
        }
      }
    }
    int size = topItems.size();
    if (size == 0) {
      return Collections.emptyList();
    }
    List<RecommendedItem> result = Lists.newArrayListWithCapacity(size);
    result.addAll(topItems);
    Collections.sort(result, ByValueRecommendedItemComparator.getInstance());
    return result;
  }
}
