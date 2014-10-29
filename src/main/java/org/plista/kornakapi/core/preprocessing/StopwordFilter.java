package org.plista.kornakapi.core.preprocessing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class StopwordFilter {

	/**
	 * Array containing all stop words
	 */
	private HashSet<String> mStopwordsSet;
	
	/**
	 * Loads the stop word list.
	 * 
	 * @throws IOException
	 */
	public StopwordFilter() throws IOException {
		mStopwordsSet = new HashSet<String>();
		BufferedReader in = new BufferedReader(new FileReader("data/kornakapi_sw_de.txt"));
		String line = in.readLine();
		while(line != null) {
			mStopwordsSet.add(line.trim());
			line = in.readLine();
		};
		in.close();
	}
	
	/**
	 * Lower cases and filters the text with the stop word list.
	 * 
	 * @param pInput
	 * @return
	 * @throws IOException
	 */
	public String filterText(String pInput) throws IOException {
		pInput = pInput.toLowerCase();
		StringWriter sw = new StringWriter();
		StringTokenizer st = new StringTokenizer(pInput);
		while(st.hasMoreTokens()) {
			String token = st.nextToken();
			if(!mStopwordsSet.contains(token)) {
				sw.write(token);
				sw.write(' ');
			} 
		}
		return sw.toString().trim();
	}
	
	/**
	 * Getter for stop words set
	 * @return
	 */
	public Set<String> getStopwords() {
		return mStopwordsSet;
	}
	
	
}
