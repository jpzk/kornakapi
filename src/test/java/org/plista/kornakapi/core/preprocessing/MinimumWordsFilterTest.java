package org.plista.kornakapi.core.preprocessing;

import junit.framework.TestCase;

public class MinimumWordsFilterTest extends TestCase {
	public void testIsValid() {
		String input = "Hello my name is";
		MinimumWordsFilter filter = new MinimumWordsFilter(3);
		assertTrue(filter.isValid(input));
		
		String input2 = "Hello";
		assertTrue(!filter.isValid(input2));
	}
}
