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
import org.plista.kornakapi.core.config.LDARecommenderConfig;
import org.plista.kornakapi.web.Components;

import com.google.common.base.Preconditions;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

/**
 * This class overwrite the TopItems of Mahout, it does prefiltering and just
 * considers items which are active (weight = 0).
 */
public final class CustomTopItems {

	private static final long[] NO_IDS = new long[0];
	
	private int mLastDays;
	private Connection mConnection;

	public CustomTopItems() throws SQLException {

		Components c = Components.instance();
		LDARecommenderConfig config = (LDARecommenderConfig) c
				.getConfiguration().getLDARecommender();
		
		String conStr = "jdbc:mysql://" + config.getDbHost() + "/db_youfilter?user=" +
				config.getDbUser() + "&password=" + config.getDbPassword();
		
		// Open connection to item database
		this.mConnection = (Connection) DriverManager.getConnection(conStr);
	};

	/**
	 * @param pItemId
	 * @return
	 * @throws SQLException
	 */
	private boolean isRecentItem(long pItemId) throws SQLException {
		Statement stmt = (Statement) this.mConnection.createStatement();
	
		ResultSet rs = stmt
				.executeQuery("SELECT COUNT(itemid) AS c from item WHERE itemid = "
						+ pItemId
						+ " and weight = 0");
		rs.next();
		int c = rs.getInt("c");
		return c > 0;
	}

	public List<RecommendedItem> getTopItems(int howMany,
			LongPrimitiveIterator possibleItemIDs, IDRescorer rescorer,
			Estimator<Long> estimator) throws TasteException, SQLException {
		Preconditions.checkArgument(possibleItemIDs != null,
				"possibleItemIDs is null");
		Preconditions.checkArgument(estimator != null, "estimator is null");

		Queue<RecommendedItem> topItems = new PriorityQueue<RecommendedItem>(
				howMany + 1,
				Collections.reverseOrder(ByValueRecommendedItemComparator
						.getInstance()));
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
				double rescoredPref = rescorer == null ? preference : rescorer
						.rescore(itemID, preference);
				if (!Double.isNaN(rescoredPref)
						&& (!full || rescoredPref > lowestTopValue)) {

					if (isRecentItem(itemID)) {
						topItems.add(new GenericRecommendedItem(itemID,
								(float) rescoredPref));
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
		Collections
				.sort(result, ByValueRecommendedItemComparator.getInstance());
		return result;
	}
}
