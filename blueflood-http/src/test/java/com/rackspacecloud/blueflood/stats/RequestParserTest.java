package com.rackspacecloud.blueflood.stats;

import org.junit.Before;
import org.junit.Test;

public class RequestParserTest {

	@Before
	public void setup() {

	}

	@Test
	public void testSumFunction() {
		String json = "{" +
				"\"targets\": [" +
					"{" +
						"\"name\": \"sum\"," +
						"\"parameters\": [" +
							"{" +
								"\"tenantId\": \"threadid\"," +
								"\"metricName\": \"metricid\"" +
							"}" +
						"]" +
					"}" +
				"]," +
				"\"from\": 1426599291591," +
				"\"to\": 1426599591592," +
				"\"maxDataPoints\": 947 " +
			"}";
		
	}

}
