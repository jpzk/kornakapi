package org.plista.kornakapi.core.training;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.commons.dbcp.BasicDataSource;
import org.plista.kornakapi.core.cluster.StreamingKMeansClassifierModel;
import org.plista.kornakapi.core.config.Configuration;
import org.plista.kornakapi.core.storage.MySqlMaxPersistentStorage;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class ExtractTest {
	
	public static void main(String [] args) throws IOException{
		/**
		 * test class
		 */
		String path = args[0];
		System.out.print(path);
		
		File configFile = new File(path);
		System.out.print(configFile.canRead());
		Configuration conf = null;
		try {
			conf = Configuration.fromXML(Files.toString(configFile, Charsets.UTF_8));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BasicDataSource dataSource = new BasicDataSource();
	    MySqlMaxPersistentStorage labelsGet = new MySqlMaxPersistentStorage(conf.getStorageConfiguration(), "",dataSource);
	    LinkedList<String> labels = labelsGet.getAllLabels();
	    labelsGet.close();
		StreamingKMeansClustererTrainer clusterer = null;
		StreamingKMeansClassifierModel model = new StreamingKMeansClassifierModel(conf.getStorageConfiguration(), labels.getFirst(), dataSource);
		try {
			clusterer = new StreamingKMeansClustererTrainer(conf.getStreamingKMeansClusterer().iterator().next(), model);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			clusterer.doTrain(configFile, null, 0);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
