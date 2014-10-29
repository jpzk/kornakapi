package org.plista.kornakapi.core.preprocessing;

import org.junit.Test;

import junit.framework.TestCase;

public class BadcharFilterTest extends TestCase {

	@Test
	public void testFilter() {
		BadcharFilter filter = new BadcharFilter();
		assertEquals(filter.filterText("ühere are som; b3d ch,,arcters"), "ühere are som bd charcters");
	}
}
