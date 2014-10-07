package org.plista.kornakapi.core.optimizer;

import java.io.File;
import java.io.IOException;

import org.plista.kornakapi.core.config.Configuration;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class OptimizerTest {
	
	
	public static void main(String [] arg) throws IOException{
		FactorizationBasedInMemoryOptimizer opti = new FactorizationBasedInMemoryOptimizer();	
		String path = "kornakapi.conf";
		File configFile = new File(path);
		System.out.print(configFile.canRead());
		Configuration conf = null;		
		conf = Configuration.fromXML(Files.toString(configFile, Charsets.UTF_8));	
		String modelPath = "/opt/kornakapi-optimizer/";
		File modelDirectory = new File(modelPath);		
		opti.optimize(modelDirectory, conf,  1,  "2866");
	}
}
