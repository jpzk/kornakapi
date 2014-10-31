package org.plista.kornakapi.core.preprocessing;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.junit.Test;
import org.plista.kornakapi.core.config.Configuration;
import org.plista.kornakapi.core.config.LDARecommenderConfig;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import junit.framework.TestCase;

public class StopwordFilterTest extends TestCase {

	@Test
	public void testFilterText() throws IOException {
		String path = "/etc/kornakapi/kornakapi.conf";
		File configFile = new File(path);
		System.out.print(configFile.canRead());
		Configuration conf =Configuration.fromXML(Files.toString(configFile, Charsets.UTF_8));	
		
    	LDARecommenderConfig config = (LDARecommenderConfig) conf.getLDARecommender();
    	String basepath = config.getPreprocessingDataDirectory();
		
		StopwordFilter swf = new StopwordFilter(basepath + "kornakapi_sw_de.txt");
		String output = swf.filterText("Das ist ein Test");
		assertEquals(output, "test");
	}
}
