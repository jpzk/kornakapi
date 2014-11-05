package org.plista.kornakapi.core.preprocessing;

import java.util.StringTokenizer;

public class MinimumWordsFilter {

	private int minimumWords;
	
	public MinimumWordsFilter(int pMinimumWords) {
		minimumWords = pMinimumWords;
	}
	
	public boolean isValid(String pInput) {
		StringTokenizer st = new StringTokenizer(pInput);
		
		int words = 0;
		while(st.hasMoreTokens()) {
			st.nextToken();
			words += 1;
		}	
		
		return words >= minimumWords;
	}
}
