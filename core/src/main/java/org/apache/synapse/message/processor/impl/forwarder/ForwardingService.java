/**
 *  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.synapse.message.processor.impl.forwarder;

import java.util.Map;
import java.util.Set;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.ManagedLifecycle;
import org.apache.synapse.Mediator;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.SynapseException;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.endpoints.Endpoint;
import org.apache.synapse.message.MessageConsumer;
import org.apache.synapse.message.processor.MessageProcessor;
import org.apache.synapse.message.processor.MessageProcessorConstants;
import org.apache.synapse.message.processor.impl.ScheduledMessageProcessor;
import org.apache.synapse.message.senders.blocking.BlockingMsgSender;
import org.apache.synapse.task.Task;
import org.apache.synapse.util.MessageHelper;

public class ForwardingService implements Task, ManagedLifecycle {
	private static final Log log = LogFactory.getLog(ForwardingService.class);

	/** The consumer that is associated with the particular message store */
	private MessageConsumer messageConsumer;

	/** Owner of the this job */
	private final MessageProcessor messageProcessor;

	/** This is the client which sends messages to the end point */
	private final BlockingMsgSender sender;

	/**
	 * Interval between two retries to the client. This only come to affect only
	 * if the client is un-reachable
	 */
	private int retryInterval = 1000;

	/** Sequence to invoke in a failure */
	private String faultSeq = null;

	/** Sequence to reply on success */
	private String replySeq = null;

	private String targetEndpoint = null;

	/**
	 * This is specially used for REST scenarios where http status codes can
	 * take semantics in a RESTful architecture.
	 */
	private String[] nonRetryStatusCodes = null;

	/**
	 * These two maintain the state of service. For each iteration these should
	 * be reset
	 */
	private boolean isSuccessful = false;
	private volatile boolean isTerminated = false;

	/**
	 * Number of retries before shutting-down the processor. -1 default value
	 * indicates that retry should happen forever
	 */
	private int maxDeliverAttempts = -1;
	private int attemptCount = 0;

	private boolean isThrottling = true;

	/**
	 * Throttling-interval is the forwarding interval when cron scheduling is
	 * enabled.
	 */
	private long throttlingInterval = -1;

	// Message Queue polling interval value.
	private long interval = ScheduledMessageProcessor.THRESHOULD_INTERVAL;

	/**
	 * Configuration to continue the message processor even without stopping the
	 * message processor after maximum number of delivery
	 */
	private boolean isMaxDeliveryAttemptDropEnabled = false;

	private SynapseEnvironment synapseEnvironment;

	private boolean initialized = false;

	public ForwardingService(MessageProcessor messageProcessor,
			BlockingMsgSender sender, SynapseEnvironment synapseEnvironment) {
		this.messageProcessor = messageProcessor;
		this.sender = sender;
		this.synapseEnvironment = synapseEnvironment;
	}

	/**
	 * Helper method to get a value of a parameters in the AxisConfiguration
	 *
	 * @param axisConfiguration
	 *            AxisConfiguration instance
	 * @param paramKey
	 *            The name / key of the parameter
	 * @return The value of the parameter
	 */
	private static String getAxis2ParameterValue(
			AxisConfiguration axisConfiguration, String paramKey) {

		Parameter parameter = axisConfiguration.getParameter(paramKey);
		if (parameter == null) {
			return null;
		}
		Object value = parameter.getValue();
		if (value != null && value instanceof String) {
			return (String) parameter.getValue();
		} else {
			return null;
		}
	}

	public MessageContext fetch(MessageConsumer msgConsumer) {
		return messageConsumer.receive();
	}

	public boolean dispatch(MessageContext messageContext) {

		if (log.isDebugEnabled()) {
			log.debug("Sending the message to client with message processor ["
					+ messageProcessor.getName() + "]");
		}

		if (targetEndpoint == null) {
			targetEndpoint = (String) messageContext
					.getProperty(ForwardingProcessorConstants.TARGET_ENDPOINT);
		}

		MessageContext outCtx = null;
		SOAPEnvelope originalEnvelop = messageContext.getEnvelope();

		if (targetEndpoint != null) {
			Endpoint ep = messageContext.getEndpoint(targetEndpoint);

			try {

				// Send message to the client
				while (!isSuccessful && !isTerminated) {
					try {
						// For each retry we need to have a fresh copy of the
						// actual message. otherwise retry may not
						// work as expected.
						OMElement firstChild = null; //
						org.apache.axis2.context.MessageContext origAxis2Ctx = ((Axis2MessageContext) messageContext)
								.getAxis2MessageContext();
						if (JsonUtil.hasAJsonPayload(origAxis2Ctx)) {
							firstChild = origAxis2Ctx.getEnvelope().getBody()
									.getFirstElement();
						} // Had to do this because
							// MessageHelper#cloneSOAPEnvelope does not clone
							// OMSourcedElemImpl correctly.
						messageContext.setEnvelope(MessageHelper
								.cloneSOAPEnvelope(originalEnvelop));
						if (JsonUtil.hasAJsonPayload(firstChild)) { //
							OMElement clonedFirstElement = messageContext
									.getEnvelope().getBody().getFirstElement();
							if (clonedFirstElement != null) {
								clonedFirstElement.detach();
								messageContext.getEnvelope().getBody()
										.addChild(firstChild);
							}
						}// Had to do this because
							// MessageHelper#cloneSOAPEnvelope does not clone
							// OMSourcedElemImpl correctly.
						outCtx = sender.send(ep, messageContext);
						isSuccessful = true;

					} catch (Exception e) {

						// this means send has failed due to some reason so we
						// have to retry it
						if (e instanceof SynapseException) {
							isSuccessful = isNonRetryErrorCode(e.getCause()
									.getMessage());
						}
						if (!isSuccessful) {
							log.error("BlockingMessageSender of message processor ["
									+ this.messageProcessor.getName()
									+ "] failed to send message to the endpoint");
						}
					}

					if (isSuccessful) {
						if (outCtx != null) {
							if ("true"
									.equals(outCtx
											.getProperty(ForwardingProcessorConstants.BLOCKING_SENDER_ERROR))) {

								// this means send has failed due to some reason
								// so we have to retry it
								isSuccessful = isNonRetryErrorCode((String) outCtx
										.getProperty(SynapseConstants.ERROR_MESSAGE));

								if (isSuccessful) {
									sendThroughReplySeq(outCtx);
								} else {
									// This means some error has occurred so
									// must try to send down the fault sequence.
									log.error("BlockingMessageSender of message processor ["
											+ this.messageProcessor.getName()
											+ "] failed to send message to the endpoint");
									sendThroughFaultSeq(outCtx);
								}
							} else {
								// Send the message down the reply sequence if
								// there is one
								sendThroughReplySeq(outCtx);
								messageConsumer.ack();
								attemptCount = 0;
								isSuccessful = true;

								if (log.isDebugEnabled()) {
									log.debug("Successfully sent the message to endpoint ["
											+ ep.getName()
											+ "]"
											+ " with message processor ["
											+ messageProcessor.getName() + "]");
								}
							}
						} else {
							// This Means we have invoked an out only operation
							// remove the message and reset the count
							messageConsumer.ack();
							attemptCount = 0;
							isSuccessful = true;

							if (log.isDebugEnabled()) {
								log.debug("Successfully sent the message to endpoint ["
										+ ep.getName()
										+ "]"
										+ " with message processor ["
										+ messageProcessor.getName() + "]");
							}
						}
					}

					if (!isSuccessful) {
						// Then we have to retry sending the message to the
						// client.
						prepareToRetry();
					} else {
						if (messageProcessor.isPaused()) {
							this.messageProcessor.resumeService();
							log.info("Resuming the service of message processor ["
									+ messageProcessor.getName() + "]");
						}
					}
				}
			} catch (Exception e) {
				log.error("Message processor [" + messageProcessor.getName()
						+ "] failed to send the message to" + " client", e);
			}
		} else {
			// No Target Endpoint defined for the Message
			// So we do not have a place to deliver.
			// Here we log a warning and remove the message
			// TODO: we can improve this by implementing a target inferring
			// mechanism

			log.warn("Property "
					+ ForwardingProcessorConstants.TARGET_ENDPOINT
					+ " not found in the message context , Hence removing the message ");

			messageConsumer.ack();
		}

		return true;
	}

	public void sendThroughFaultSeq(MessageContext msgCtx) {
		if (faultSeq == null) {
			log.warn("Failed to send the message through the fault sequence, Sequence name "
					+ faultSeq + " does not Exist.");
			return;
		}
		Mediator mediator = msgCtx.getSequence(faultSeq);

		if (mediator == null) {
			log.warn("Failed to send the message through the fault sequence, Sequence object"
					+ faultSeq + " does not Exist.");
			return;
		}

		mediator.mediate(msgCtx);
	}

	public void sendThroughReplySeq(MessageContext outCtx) {
		if (replySeq == null) {
			this.messageProcessor.deactivate();
			log.error("Can't Send the Out Message , Sequence name " + replySeq
					+ " does not Exist. Deactivated the" + " message processor");
			return;
		}
		Mediator mediator = outCtx.getSequence(replySeq);

		if (mediator == null) {
			this.messageProcessor.deactivate();
			log.error("Can't Send the Out Message , Sequence object "
					+ replySeq + " does not Exist. Deactivated the"
					+ " message processor");
			return;
		}

		mediator.mediate(outCtx);
	}

	public boolean terminate() {
		try {
			isTerminated = true;
			Thread.currentThread().interrupt();

			if (log.isDebugEnabled()) {
				log.debug("Successfully terminated job of message processor ["
						+ messageProcessor.getName() + "]");
			}
			return true;
		} catch (Exception e) {
			log.error("Failed to terminate the job of message processor ["
					+ messageProcessor.getName() + "]");
			return false;
		}
	}

	private void checkAndDeactivateProcessor() {
		if (maxDeliverAttempts > 0) {
			this.attemptCount++;

			if (attemptCount >= maxDeliverAttempts) {
				terminate();
				this.messageProcessor.deactivate();

				if (this.isMaxDeliveryAttemptDropEnabled) {
					dropMessageAndContinueMessageProcessor();
					if (log.isDebugEnabled()) {
						log.debug("Message processor ["
								+ messageProcessor.getName()
								+ "] Dropped the failed message and continue due to reach of max attempts");
					}
				} else {
					terminate();
					this.messageProcessor.deactivate();

					if (log.isDebugEnabled()) {
						log.debug("Message processor ["
								+ messageProcessor.getName()
								+ "] stopped due to reach of max attempts");
					}
				}
			}
		}
	}

	private void prepareToRetry() {
		if (!isTerminated) {
			// First stop the processor since no point in re-triggering jobs if
			// the we can't send
			// it to the client
			if (!messageProcessor.isPaused()) {
				this.messageProcessor.pauseService();

				log.info("Pausing the service of message processor ["
						+ messageProcessor.getName() + "]");
			}

			checkAndDeactivateProcessor();

			if (log.isDebugEnabled()) {
				log.debug("Failed to send to client retrying after "
						+ retryInterval + "s with attempt count - "
						+ attemptCount);
			}

			try {
				// wait for some time before retrying
				Thread.sleep(retryInterval);
			} catch (InterruptedException ignore) {
				// No harm even it gets interrupted. So nothing to handle.
			}
		}
	}

	private void resetService() {
		isSuccessful = false;
		attemptCount = 0;
	}

	private boolean isNonRetryErrorCode(String errorMsg) {
		boolean isSuccess = false;
		if (nonRetryStatusCodes != null) {
			for (String code : nonRetryStatusCodes) {
				if (errorMsg != null && errorMsg.contains(code)) {
					isSuccess = true;
					break;
				}
			}
		}

		return isSuccess;
	}

	private boolean isRunningUnderCronExpression() {
		return this.isThrottling && (throttlingInterval > -1);
	}

	/**
	 * This method is enable to the
	 */
	private void dropMessageAndContinueMessageProcessor() {
		messageConsumer.ack();
		attemptCount = 0;
		isSuccessful = true;
		if (this.messageProcessor.isPaused()) {
			this.messageProcessor.resumeService();
		}
		log.info("Removed failed message and continue the message processor ["
				+ this.messageProcessor.getName() + "]");
	}

	public void init(SynapseEnvironment se) {
		// Setting up the JMS consumer here.
		setMessageConsumer();

		// Defaults to -1.
		Map<String, Object> parametersMap = messageProcessor.getParameters();
		if (parametersMap.get(MessageProcessorConstants.MAX_DELIVER_ATTEMPTS) != null) {
			maxDeliverAttempts = Integer.parseInt((String) parametersMap
					.get(MessageProcessorConstants.MAX_DELIVER_ATTEMPTS));
		}

		if (parametersMap.get(MessageProcessorConstants.RETRY_INTERVAL) != null) {
			retryInterval = Integer.parseInt((String) parametersMap
					.get(MessageProcessorConstants.RETRY_INTERVAL));
		}

		replySeq = (String) parametersMap
				.get(ForwardingProcessorConstants.REPLY_SEQUENCE);

		faultSeq = (String) parametersMap
				.get(ForwardingProcessorConstants.FAULT_SEQUENCE);

		targetEndpoint = (String) parametersMap
				.get(ForwardingProcessorConstants.TARGET_ENDPOINT);

		// Default value should be true.
		if (parametersMap.get(ForwardingProcessorConstants.THROTTLE) != null) {
			isThrottling = Boolean.parseBoolean((String) parametersMap
					.get(ForwardingProcessorConstants.THROTTLE));
		}

		// Default Value should be -1.
		if (isThrottling
				&& parametersMap
						.get(ForwardingProcessorConstants.THROTTLE_INTERVAL) != null) {
			throttlingInterval = Long.parseLong((String) parametersMap
					.get(ForwardingProcessorConstants.THROTTLE_INTERVAL));
		}

		nonRetryStatusCodes = (String[]) parametersMap
				.get(ForwardingProcessorConstants.NON_RETRY_STATUS_CODES);

		// Default to FALSE.
		if (parametersMap.get(ForwardingProcessorConstants.MAX_DELIVERY_DROP) != null
				&& parametersMap
						.get(ForwardingProcessorConstants.MAX_DELIVERY_DROP)
						.toString().equals("Enabled") && maxDeliverAttempts > 0) {
			isMaxDeliveryAttemptDropEnabled = true;
		}

		// Setting the interval value.
		interval = Long.parseLong((String) parametersMap
				.get(MessageProcessorConstants.INTERVAL));

		/*
		 * Make sure to set the isInitialized flag too TRUE in order to avoid
		 * re-initialization.
		 */
		initialized = true;
	}

	public void destroy() {
		terminate();

	}

	public void execute() {
		/*
		 * Initialize only if it is NOT already done. This will make sure that
		 * the initialization is done only once.
		 */
		if (!initialized) {
			this.init(synapseEnvironment);
		}

		do {
			resetService();

			try {
				if (!this.messageProcessor.isDeactivated()) {
					MessageContext messageContext = fetch(messageConsumer);

					if (messageContext != null) {

						String serverName = (String) messageContext
								.getProperty(SynapseConstants.Axis2Param.SYNAPSE_SERVER_NAME);

						if (serverName != null
								&& messageContext instanceof Axis2MessageContext) {

							AxisConfiguration configuration = ((Axis2MessageContext) messageContext)
									.getAxis2MessageContext()
									.getConfigurationContext()
									.getAxisConfiguration();

							String myServerName = getAxis2ParameterValue(
									configuration,
									SynapseConstants.Axis2Param.SYNAPSE_SERVER_NAME);

							if (!serverName.equals(myServerName)) {
								return;
							}
						}

						Set proSet = messageContext.getPropertyKeySet();

						if (proSet != null) {
							if (proSet
									.contains(ForwardingProcessorConstants.BLOCKING_SENDER_ERROR)) {
								proSet.remove(ForwardingProcessorConstants.BLOCKING_SENDER_ERROR);
							}
						}

						// Now it is NOT terminated anymore.
						isTerminated = messageProcessor.isDeactivated();
						dispatch(messageContext);

					} else {
						// either the connection is broken or there are no new
						// massages.
						if (log.isDebugEnabled()) {
							log.debug("No messages were received for message processor ["
									+ messageProcessor.getName() + "]");
						}

						// this means we have consumed all the messages
						if (isRunningUnderCronExpression()) {
							break;
						}
					}
				} else {

					// we need this because when start the server while the
					// processors in deactivated mode
					// the deactivation may not come in to play because the
					// service may not be running.
					isTerminated = true;

					if (log.isDebugEnabled()) {
						log.debug("Exiting service since the message processor is deactivated");
					}
				}
			} catch (Throwable e) {
				// All the possible recoverable exceptions are handles case by
				// case and yet if it comes this
				// we have to shutdown the processor
				log.fatal("Deactivating the message processor ["
						+ this.messageProcessor.getName() + "]", e);

				this.messageProcessor.deactivate();
			}

			if (log.isDebugEnabled()) {
				log.debug("Exiting the iteration of message processor ["
						+ this.messageProcessor.getName() + "]");
			}

			// This code wrote handle scenarios in which cron expressions are
			// used
			// for scheduling task
			if (isRunningUnderCronExpression()) {
				try {
					Thread.sleep(throttlingInterval);
				} catch (InterruptedException e) {
					// no need to worry. it does have any serious consequences
				}
			}

			/*
			 * If the interval is less than 1000 ms, then the scheduling is done
			 * using the while loop since ntask rejects any intervals whose
			 * value is less then 1000 ms.
			 */
			if (interval > 0 && interval < 1000) {
				try {
					Thread.sleep(interval);
				} catch (InterruptedException e) {
					log.debug("Current Thread was interrupted while it is sleeping.");
				}
			}

		} while (isThrottling && !isTerminated);

		if (log.isDebugEnabled()) {
			log.debug("Exiting service thread of message processor ["
					+ this.messageProcessor.getName() + "]");
		}

	}

	private boolean setMessageConsumer() {
		final String messageStore = messageProcessor.getMessageStoreName();
		messageConsumer = synapseEnvironment.getSynapseConfiguration()
				.getMessageStore(messageStore).getConsumer();
		/*
		 * Make sure to set the same message consumer in the message processor
		 * since it is used by life-cycle management methods. Specially by the
		 * deactivate method to cleanup the connection before the deactivation.
		 */
		return messageProcessor.setMessageConsumer(messageConsumer);

	}

	/**
	 * Checks whether this TaskService is properly initialized or not.
	 * 
	 * @return <code>true</code> if this TaskService is properly initialized.
	 *         <code>false</code> otherwise.
	 */
	public boolean isInitialized() {
		return initialized;
	}

}
