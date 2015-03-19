package com.rackspacecloud.blueflood.stats;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.rackspacecloud.blueflood.exceptions.InvalidRequestException;

public class RequestParser {
	private Gson gson;
	
	public RequestParser() {
		this.gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
	}
	
	public Query parse(String body) throws InvalidRequestException {
		if (body == null || body.isEmpty()) {
			throw new InvalidRequestException("Expected JSON object.");
		}
		
		Query query = gson.fromJson(body, Query.class);
		
		if (query.getFrom() == 0) {
			throw new InvalidRequestException("Missing valid argument from.");
		}
		
		if (query.getTo() == 0) {
			throw new InvalidRequestException("Missing valid argument to.");
		}
		
		if (query.getMaxDataPoints() == 0 || query.getResolution() == null) {
			throw new InvalidRequestException("Missing valid argument maxDataPoints or resolution.");
		}
		
		if (query.getTargets() == null || query.getTargets().isEmpty()) {
			throw new InvalidRequestException("At least one target is needed.");
		}
		
		return query;
	}
}
