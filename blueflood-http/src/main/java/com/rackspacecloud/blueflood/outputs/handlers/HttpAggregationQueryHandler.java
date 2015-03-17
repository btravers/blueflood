package com.rackspacecloud.blueflood.outputs.handlers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.exceptions.InvalidRequestException;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.http.HttpResponder;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.outputs.serializers.BatchedMetricsJSONOutputSerializer;
import com.rackspacecloud.blueflood.outputs.serializers.BatchedMetricsOutputSerializer;
import com.rackspacecloud.blueflood.outputs.serializers.BasicRollupsOutputSerializer.MetricStat;
import com.rackspacecloud.blueflood.outputs.utils.RollupsQueryParams;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.HttpConfig;
import com.rackspacecloud.blueflood.types.BatchMetricsQuery;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Resolution;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;

public class HttpAggregationQueryHandler implements HttpRequestHandler {
	private static final Logger log = LoggerFactory.getLogger(HttpMultiRollupsQueryHandler.class);
    private final BatchedMetricsOutputSerializer<JSONObject> serializer;
    private final Gson gson;           // thread-safe
    private final JsonParser parser;   // thread-safe
    private final Timer httpMetricsFetchTimer = Metrics.timer(HttpRollupsQueryHandler.class, "Handle HTTP request for metrics");
    private final ThreadPoolExecutor executor;
    private final TimeValue queryTimeout;
    private final int maxMetricsPerRequest;
	
	public HttpAggregationQueryHandler() {
		Configuration config = Configuration.getInstance();
		int maxThreadsToUse = config.getIntegerProperty(HttpConfig.MAX_READ_WORKER_THREADS);
        int maxQueueSize = config.getIntegerProperty(HttpConfig.MAX_BATCH_READ_REQUESTS_TO_QUEUE);
        this.queryTimeout = new TimeValue(
                config.getIntegerProperty(HttpConfig.BATCH_QUERY_TIMEOUT),
                TimeUnit.SECONDS
        );
        this.maxMetricsPerRequest = config.getIntegerProperty(HttpConfig.MAX_METRICS_PER_BATCH_QUERY);
        this.serializer = new BatchedMetricsJSONOutputSerializer();
        this.gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
        this.parser = new JsonParser();
        this.executor = new ThreadPoolBuilder().withCorePoolSize(maxThreadsToUse).withMaxPoolSize(maxThreadsToUse)
        		.withName("HTTP-BatchMetricsFetch").withBoundedQueue(maxQueueSize).build();
	}
	
	private RollupsQueryParams parse(JsonObject obj) throws InvalidRequestException {
		
		if (obj.get("from") == null) {
			throw new InvalidRequestException("Missing argument from.");
		}
		long from = obj.get("from").getAsLong();
		
		if (obj.get("to") == null) {
			throw new InvalidRequestException("Missing argument to.");
		}
		long to = obj.get("to").getAsLong();
		
		if (to <= from) {
            throw new InvalidRequestException("paramter 'to' must be greater than 'from'");
        }
		
		Set<MetricStat> stats = new HashSet<MetricStat>();
		if (obj.get("stats") != null) {
			JsonArray statsArray = obj.get("stats").getAsJsonArray();
			Iterator<JsonElement> it = statsArray.iterator();
			while (it.hasNext()) {
				stats.add(gson.fromJson(it.next(), MetricStat.class));
			}
		}
		
		if (obj.get("points") != null) {
			int points = obj.get("points").getAsInt();
			RollupsQueryParams queryParam = new RollupsQueryParams(from, to, points, stats);
			return queryParam;
		} else if (obj.get("resolution") != null) {
			Resolution resolution = gson.fromJson(obj.get("resolution"), Resolution.class);
			RollupsQueryParams queryParam = new RollupsQueryParams(from, to, resolution, stats);
			return queryParam;
		} else {
			throw new InvalidRequestException("Missing argument points or resolution.");
		}
	}
	
	private List<Locator> getLocators(JsonArray arr) throws InvalidRequestException {
		final List<Locator> locators = new ArrayList<Locator>();
		
		Iterator<JsonElement> it = arr.iterator();
		while (it.hasNext()) {
			JsonElement elem = it.next();
			JsonObject obj = elem.getAsJsonObject();
			
			if (obj == null || obj.isJsonNull()) {
				throw new InvalidRequestException("Metrics are composed of a tenant id and a metric name");
			}
			
			String tenant = obj.get("tenant").getAsString();
			String metric = obj.get("metric").getAsString();
			if (tenant == null || metric == null) {
				throw new InvalidRequestException("Metrics are composed of a tenant id and a metric name");
			}
			
			locators.add(Locator.createLocatorFromPathComponents(tenant, metric));
		}
		
		return locators;
	}
	
	@Override
	public void handle(ChannelHandlerContext ctx, HttpRequest request) {
		final String sbody = request.getContent().toString(Constants.DEFAULT_CHARSET);

        if (sbody == null || sbody.isEmpty()) {
            sendResponse(ctx, request, "Invalid body. Expected JSON object.",
                    HttpResponseStatus.BAD_REQUEST);
            return;
        }
        
        JsonElement elem = parser.parse(sbody);
        if (elem == null) {
        	sendResponse(ctx, request, "Invalid body. Expected JSON object.",
                    HttpResponseStatus.BAD_REQUEST);
            return;
        }
        JsonObject body = elem.getAsJsonObject();
        
        JsonArray metrics = body.get("metrics").getAsJsonArray();
        
        if (metrics == null || metrics.isJsonNull() || metrics.size() == 0) {
        	sendResponse(ctx, request, "Invalid body. Expected at least one metric.", HttpResponseStatus.BAD_REQUEST);
            return;
        }
        
        List<Locator> locators;
        try {
        	locators = this.getLocators(metrics);
        } catch (Exception ex) {
            log.debug(ex.getMessage(), ex);
            sendResponse(ctx, request, ex.getMessage(), HttpResponseStatus.BAD_REQUEST);
            return;
        }
            
        if (locators.size() > maxMetricsPerRequest) {
            sendResponse(ctx, request, "Too many metrics fetch in a single call. Max limit is " + maxMetricsPerRequest
                    + ".", HttpResponseStatus.BAD_REQUEST);
            return;
        }
        
        final Timer.Context httpMetricsFetchTimerContext = httpMetricsFetchTimer.time();
           
        try {
        	RollupsQueryParams params = this.parse(body);
        	BatchMetricsQuery query = new BatchMetricsQuery(locators, params.getRange(), params.getGranularity());
        	Map<Locator, MetricData> results = new BatchMetricsQueryHandler(executor, AstyanaxReader.getInstance()).execute(query, queryTimeout);
        	
        	System.out.println(gson.toJson(results));
        	
        	JSONObject series = serializer.transformRollupData(results, params.getStats());
            final JsonElement element = parser.parse(series.toString());
            final String jsonStringRep = gson.toJson(element);
            sendResponse(ctx, request, jsonStringRep, HttpResponseStatus.OK);
		} catch (InvalidRequestException e) {
			log.warn(e.getMessage());
            sendResponse(ctx, request, "Invalid body. " + e.getMessage(), HttpResponseStatus.BAD_REQUEST);
		} catch (SerializationException e) {
			log.error(e.getMessage(), e);
            sendResponse(ctx, request, e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
            sendResponse(ctx, request, e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
		} finally {
            httpMetricsFetchTimerContext.stop();
        }
	}
	
	private void sendResponse(ChannelHandlerContext channel, HttpRequest request, String messageBody, HttpResponseStatus status) {
		HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
		
		if (messageBody != null && !messageBody.isEmpty()) {
			response.setContent(ChannelBuffers.copiedBuffer(messageBody, Constants.DEFAULT_CHARSET));
		}
		HttpResponder.respond(channel, request, response);
	}

}
