/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *   * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.synapse.message.processor.impl;

import java.util.Map;
import java.util.StringTokenizer;

import org.apache.axis2.deployment.DeploymentEngine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.deployers.MessageProcessorDeployer;
import org.apache.synapse.message.processor.MessageProcessorConstants;
import org.apache.synapse.message.processor.impl.forwarder.ForwardingProcessorConstants;
import org.apache.synapse.message.senders.blocking.BlockingMsgSender;
import org.apache.synapse.task.Task;
import org.apache.synapse.task.TaskDescription;
import org.apache.synapse.task.TaskManager;
import org.wso2.carbon.mediation.ntask.NTaskTaskManager;

public abstract class ScheduledMessageProcessor extends
		AbstractMessageProcessor {
	private static final Log logger = LogFactory
			.getLog(ScheduledMessageProcessor.class.getName());

	// Threshould interval value is 1000.
	public static final long THRESHOULD_INTERVAL = 1000;

	/**
	 * The interval at which this processor runs , default value is 1000ms
	 */
	protected long interval = THRESHOULD_INTERVAL;

	/**
	 * A cron expression to run the sampler
	 */
	protected String cronExpression = null;

	/**
	 * This only needed for the associated service. This value could not be
	 * changed manually. Moving to this state only happens when the service
	 * reaches the maximum retry limit
	 */
	// protected AtomicBoolean isPaused = new AtomicBoolean(false);

	// private AtomicBoolean isActivated = new AtomicBoolean(true);

	/**
	 * This is specially used for REST scenarios where http status codes can
	 * take semantics in a RESTful architecture.
	 */
	protected String[] nonRetryStatusCodes = null;

	protected BlockingMsgSender sender;

	protected SynapseEnvironment synapseEnvironment;

	private TaskManager nTaskManager;

	private final Object lock = new Object();

	private MessageProcessorState messageProcessorState = MessageProcessorState.OTHER;

	private int memberCount = 1;

	public final String MEMBER_COUNT = "member.count";

	public void init(SynapseEnvironment se) {
		this.synapseEnvironment = se;
		initMessageSender(parameters);
		if (!isPinnedServer(se.getServerContextInformation()
				.getServerConfigurationInformation().getServerName())) {
			// If it is not a pinned server we do not start the message
			// processor. In that server.
			setActivated(false);
		}
		super.init(se);

		nTaskManager = new NTaskTaskManager();
		nTaskManager.setName(name + " Schedular");
		nTaskManager.init(synapseEnvironment.getSynapseConfiguration()
				.getProperties());

		this.start();
	}

	public boolean start() {
		for (int i = 0; i < memberCount; i++) {
			/*
			 * Make sure to fetch the task after initializing the message sender
			 * and consumer properly. Otherwise you may get NullPointer
			 * exceptions.
			 */
			Task task = this.getTask();
			TaskDescription taskDescription = new TaskDescription();
			/*
			 * The same name should be used when deactivating, pausing,
			 * activating,deleting etc.
			 */
			if (i == 0) {
				taskDescription.setName(name);
			} else if (i > 0) {
				taskDescription.setName(name + i);
			}

			taskDescription
					.setTaskGroup(MessageProcessorConstants.SCHEDULED_MESSAGE_PROCESSOR_GROUP);
			/*
			 * If this interval value is less than 1000 ms, ntask will throw an
			 * exception while building the task. So to get around that we are
			 * setting threshold interval value of 1000 ms to the task
			 * description here. But actual interval value may be less than 1000
			 * ms, and hence isThrotling is set to TRUE.
			 */
			if (interval < THRESHOULD_INTERVAL) {
				taskDescription.setInterval(THRESHOULD_INTERVAL);
			} else {
				taskDescription.setInterval(interval);
			}
			taskDescription.setIntervalInMs(true);
			taskDescription.addResource(TaskDescription.INSTANCE, task);
			taskDescription.addResource(TaskDescription.CLASSNAME, task
					.getClass().getName());

			nTaskManager.schedule(taskDescription);

		}
		messageProcessorState = MessageProcessorState.STARTED;
		if (logger.isDebugEnabled()) {
			logger.debug("Started message processor. [" + getName() + "].");
		}
		return true;
	}

	public boolean isDeactivated() {
		final boolean deactivated;
		synchronized (lock) {
			deactivated = messageProcessorState
					.equals(MessageProcessorState.STOPPED);
		}

		return deactivated;
	}

	public void setParameters(Map<String, Object> parameters) {

		super.setParameters(parameters);

		if (parameters != null && !parameters.isEmpty()) {
			Object o = parameters
					.get(MessageProcessorConstants.CRON_EXPRESSION);
			if (o != null) {
				cronExpression = o.toString();
			}
			o = parameters.get(MessageProcessorConstants.INTERVAL);
			if (o != null) {
				interval = Integer.parseInt(o.toString());
			}
			o = parameters.get(MEMBER_COUNT);
			if (o != null) {
				memberCount = Integer.parseInt(o.toString());
			}
			o = parameters.get(MessageProcessorConstants.IS_ACTIVATED);
			if (o != null) {
				setActivated(Boolean.valueOf(o.toString()));
				// isActivated.set(Boolean.valueOf(o.toString()));
			}
			o = parameters
					.get(ForwardingProcessorConstants.NON_RETRY_STATUS_CODES);
			if (o != null) {
				// we take it out of param set and send it because we need split
				// the array.
				nonRetryStatusCodes = o.toString().split(",");
			}
		}
	}

	public boolean stop() {
		if (nTaskManager != null) {
			// There could be servers that are disabled at startup time.
			// therefore not started but initiated.
			if (nTaskManager.isInitialized()) {
				// This is to immediately stop the scheduler to avoid firing
				// new services
				nTaskManager.pause(name);

				if (logger.isDebugEnabled()) {
					logger.debug("ShuttingDown Message Processor Scheduler : "
							+ nTaskManager.getName());
				}
			}

			// gracefully shutdown
			/*
			 * This value should be given in the format --> taskname::taskgroup.
			 * Otherwise a default group is assigned by the ntask task manager.
			 */
			nTaskManager
					.delete(name
							+ "::"
							+ MessageProcessorConstants.SCHEDULED_MESSAGE_PROCESSOR_GROUP);
		}

		if (logger.isDebugEnabled()) {
			logger.debug("Stopped message processor [" + getName() + "].");
		}

		return true;
	}

	public void destroy() {
		// Since for one scheduler there is only one job, we can simply shutdown
		// the scheduler
		// which will cause to shutdown the job
		try {
			stop();
		} finally {
			// Cleaning up the message consumer goes here.
			if (getMessageConsumer() != null) {
				boolean success = getMessageConsumer().cleanup();
				if (!success) {
					logger.error("[" + getName()
							+ "] Could not cleanup message consumer.");
				}
			} else {
				logger.warn("[" + getName()
						+ "] Could not find the message consumer to cleanup.");
			}
		}

		if (logger.isDebugEnabled()) {
			logger.debug("Successfully destroyed message processor ["
					+ getName() + "].");
		}
	}

	public boolean deactivate() {
		if (nTaskManager != null && nTaskManager.isInitialized()) {
			if (logger.isDebugEnabled()) {
				logger.debug("Deactivating message processor [" + getName()
						+ "]");
			}

			// This is to immediately stop the scheduler to avoid firing new
			// services
			// pause only if it is NOT already paused.
			if (!isPaused()) {
				nTaskManager.pause(name);
				/*
				 * We are NOT setting isPaused to TRUE, since we are using pause
				 * to virtually deactivate the message processor.
				 */
			}

			// This is to remove the consumer from the queue.
			/*
			 * This will close the connection with the JMS Provider/message
			 * store.
			 */
			if (messageConsumer != null) {
				messageConsumer.cleanup();
			}

			logger.info("Successfully deactivated the message processor ["
					+ getName() + "]");

			synchronized (lock) {
				messageProcessorState = MessageProcessorState.STOPPED;
			}

			setActivated(false);

			// This means the deactivation has happened automatically. So we
			// have to persist the
			// deactivation manually.
			if (isPaused()) {
				try {
					// TODO: Need to make sure if this is the best way.
					String directory = configuration.getPathToConfigFile()
							+ "/message-processors";
					DeploymentEngine deploymentEngine = (DeploymentEngine) configuration
							.getAxisConfiguration().getConfigurator();
					MessageProcessorDeployer dep = (MessageProcessorDeployer) deploymentEngine
							.getDeployer(directory, "xml");
					dep.restoreSynapseArtifact(name);
				} catch (Exception e) {
					logger.warn("Couldn't persist the state of the message processor ["
							+ name + "]");
				}
			}

			return true;
		} else {
			return false;
		}
	}

	public boolean activate() {
		if (messageConsumer == null) {
			// This is for the message processors who are deactivated at
			// startup time.
			setMessageConsumer(configuration.getMessageStore(messageStore)
					.getConsumer());
		}

		/*
		 * Checking whether it is already deactivated. If it is deactivated only
		 * we can activate again.
		 */
		final boolean deactivated;

		synchronized (lock) {
			deactivated = messageProcessorState
					.equals(MessageProcessorState.STOPPED);
		}
		if (nTaskManager != null && deactivated) {
			if (logger.isDebugEnabled()) {
				logger.debug("Starting Message Processor Scheduler : "
						+ nTaskManager.getName());
			}

			resumeService();

			logger.info("Successfully re-activated the message processor ["
					+ getName() + "]");

			setActivated(true);

			return true;
		} else {
			return false;
		}
	}

	public void pauseService() {
		nTaskManager.pause(name);
		// this.isPaused.set(true);

		synchronized (lock) {
			messageProcessorState = MessageProcessorState.PAUSED;
		}
	}

	public void resumeService() {
		nTaskManager.resume(name);
		// this.isPaused.set(false);

		synchronized (lock) {
			messageProcessorState = MessageProcessorState.STARTED;
		}
	}

	public boolean isActive() {
		return !isDeactivated();
	}

	public boolean isPaused() {
		return messageProcessorState.equals(MessageProcessorState.PAUSED);
		// return isPaused.get();
	}

	public boolean getActivated() {
		return messageProcessorState.equals(MessageProcessorState.STARTED);
		// return isActivated.get();
	}

	public void setActivated(boolean activated) {
		// isActivated.set(activated);
		if (activated) {
			messageProcessorState = MessageProcessorState.STARTED;
		} else {
			messageProcessorState = MessageProcessorState.STOPPED;
		}
		parameters.put(MessageProcessorConstants.IS_ACTIVATED,
				String.valueOf(activated));
	}

	private boolean isPinnedServer(String serverName) {
		boolean pinned = false;
		Object pinnedServersObj = this.parameters
				.get(MessageProcessorConstants.PINNED_SERVER);

		if (pinnedServersObj != null && pinnedServersObj instanceof String) {

			String pinnedServers = (String) pinnedServersObj;
			StringTokenizer st = new StringTokenizer(pinnedServers, " ,");

			while (st.hasMoreTokens()) {
				String token = st.nextToken().trim();
				if (serverName.equals(token)) {
					pinned = true;
					break;
				}
			}
			if (!pinned) {
				logger.info("Message processor '" + name + "' pinned on '"
						+ pinnedServers + "' not starting on"
						+ " this server '" + serverName + "'");
			}
		} else {
			// this means we have to use the default value that is to start the
			// message processor
			pinned = true;
		}

		return pinned;
	}

	/**
	 * ntask does not accept intervals less than 1000ms as its schedule
	 * interval. Therefore when the interval is less than 1000ms, we have to
	 * handle it as a separate case. Here we run the thread in a loop and sleep
	 * the current thread for a given interval in order to manage the scheduling
	 * interval.
	 * 
	 * @param interval
	 *            in which scheduler triggers its job.
	 * @return true if it has run on non-throttle mode.
	 */
	protected boolean isThrottling(long interval) {
		return interval < THRESHOULD_INTERVAL;
	}

	protected boolean isThrottling(String cronExpression) {
		return cronExpression != null;
	}

	private BlockingMsgSender initMessageSender(Map<String, Object> params) {

		String axis2repo = (String) params
				.get(ForwardingProcessorConstants.AXIS2_REPO);
		String axis2Config = (String) params
				.get(ForwardingProcessorConstants.AXIS2_CONFIG);

		sender = new BlockingMsgSender();
		if (axis2repo != null) {
			sender.setClientRepository(axis2repo);
		}
		if (axis2Config != null) {
			sender.setAxis2xml(axis2Config);
		}
		sender.init();

		return sender;
	}

	/**
	 * Retrieves the message processors current state.
	 * 
	 * @return current state of the message processor.
	 */
	public MessageProcessorState getMessageProcessorState() {
		return messageProcessorState;
	}

	/**
	 * Gives the {@link Task} instance associated with this processor.
	 * 
	 * @return {@link Task} associated with this processor.
	 */
	protected abstract Task getTask();

}
