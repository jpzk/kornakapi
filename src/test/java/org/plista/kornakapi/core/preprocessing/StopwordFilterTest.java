package org.plista.kornakapi.core.preprocessing;

import java.io.IOException;
import java.util.Set;

import org.junit.Test;

import junit.framework.TestCase;

public class StopwordFilterTest extends TestCase {

	@Test
	public void testListRead() throws IOException {
		StopwordFilter swf = new StopwordFilter();
		Set<String> stopwords = swf.getStopwords();
		assertTrue(stopwords.size() > 0);
	}

	@Test
	public void testFilterText() throws IOException {
		StopwordFilter swf = new StopwordFilter();
		String output = swf.filterText("Das ist ein Test");
		assertEquals(output, "test");
	}
}
