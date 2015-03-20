package com.rackspacecloud.blueflood.stats;

import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.gson.JsonElement;
import com.rackspacecloud.blueflood.exceptions.InvalidRequestException;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.http.HttpResponder;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.outputs.handlers.HttpMultiRollupsQueryHandler;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.HttpConfig;
import com.rackspacecloud.blueflood.utils.Metrics;

public class HttpStatsQueryHandler implements HttpRequestHandler {
	private static final Logger log = LoggerFactory.getLogger(HttpMultiRollupsQueryHandler.class);
	private final Timer httpMetricsFetchTimer = Metrics.timer(HttpStatsQueryHandler.class, "Handle HTTP request for metrics");
	private RequestParser parser;
	private final int maxMetricsPerRequest;
	
	public HttpStatsQueryHandler() {
		Configuration config = Configuration.getInstance();
		
		parser = new RequestParser();
		maxMetricsPerRequest = config.getIntegerProperty(HttpConfig.MAX_METRICS_PER_BATCH_QUERY);
	}
	
	@Override
	public void handle(ChannelHandlerContext ctx, HttpRequest request) {
		final String body = request.getContent().toString(Constants.DEFAULT_CHARSET);
		
		Query query;
		try {
			query = parser.parse(body);
		} catch (InvalidRequestException e) {
			log.error(e.getMessage(), e);
			sendResponse(ctx, request, "Invalid body. " + e.getMessage(), HttpResponseStatus.BAD_REQUEST);
            return;
		}
		
		if (query.getTargets().size() > maxMetricsPerRequest) {
			sendResponse(ctx, request, "Too many metrics fetch in a single call. Max limit is " + maxMetricsPerRequest
                    + ".", HttpResponseStatus.BAD_REQUEST);
            return;
		}
		
		final Timer.Context httpMetricsFetchTimerContext = httpMetricsFetchTimer.time();
		RollupRequest rollup = new RollupRequest(query);
		try {
			Map<Target, MetricData> results = rollup.getData();
			JsonElement response = ResponseSerializer.serializeResponse(results);
			sendResponse(ctx, request, response.toString(), HttpResponseStatus.OK);
		} catch (SerializationException e) {
			log.error(e.getMessage(), e);
            sendResponse(ctx, request, "Invalid body. " + e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
		} catch (TargetTypeException e) {
			log.error(e.getMessage(), e);
            sendResponse(ctx, request, e.getMessage(), HttpResponseStatus.BAD_REQUEST);
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
