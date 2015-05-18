package com.rackspacecloud.blueflood.stats;

import java.util.List;

import com.rackspacecloud.blueflood.types.Resolution;

public class Query {
	private long from;
	private long to;
	private int points;
	private Resolution resolution;
	private List<Target> targets;

	public long getFrom() {
		return from;
	}

	public void setFrom(long from) {
		this.from = from;
	}

	public long getTo() {
		return to;
	}

	public void setTo(long to) {
		this.to = to;
	}

	public int getPoints() {
		return points;
	}

	public void setPoints(int points) {
		this.points = points;
	}

	public Resolution getResolution() {
		return this.resolution;
	}

	public void SetResolution(Resolution resolution) {
		this.resolution = resolution;
	}

	public List<Target> getTargets() {
		return targets;
	}

	public void setTargets(List<Target> targets) {
		this.targets = targets;
	}
}
