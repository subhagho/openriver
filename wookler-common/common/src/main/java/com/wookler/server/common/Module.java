package com.wookler.server.common;

// TODO: Auto-generated Javadoc

import com.wookler.server.common.config.CParam;
import com.wookler.server.common.config.CPath;

/**
 * Created by subho on 10/11/15.
 */
@CPath(path = "module")
public class Module {

	/** The instance id. */
	private String instanceId;

	@CParam(name = "@name")
	/** The name. */
	private String name;

	/** The start time. */
	private long startTime;

	/** The hostname. */
	private String hostname;

	/** The hostip. */
	private String hostip;

	/**
	 * Gets the instance id.
	 *
	 * @return the instance id
	 */
	public String getInstanceId() {
		return instanceId;
	}

	/**
	 * Sets the instance id.
	 *
	 * @param instanceId
	 *            the new instance id
	 */
	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}

	/**
	 * Gets the name.
	 *
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Sets the name.
	 *
	 * @param name
	 *            the new name
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Gets the start time.
	 *
	 * @return the start time
	 */
	public long getStartTime() {
		return startTime;
	}

	/**
	 * Sets the start time.
	 *
	 * @param startTime
	 *            the new start time
	 */
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	/**
	 * Gets the hostname.
	 *
	 * @return the hostname
	 */
	public String getHostname() {
		return hostname;
	}

	/**
	 * Sets the hostname.
	 *
	 * @param hostname
	 *            the new hostname
	 */
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	/**
	 * Gets the hostip.
	 *
	 * @return the hostip
	 */
	public String getHostip() {
		return hostip;
	}

	/**
	 * Sets the hostip.
	 *
	 * @param hostip
	 *            the new hostip
	 */
	public void setHostip(String hostip) {
		this.hostip = hostip;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("[%s][%s][%s %s]", name, instanceId, hostname,
				hostip);
	}

}
