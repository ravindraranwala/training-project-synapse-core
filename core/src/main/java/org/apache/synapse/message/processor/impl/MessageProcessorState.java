package org.apache.synapse.message.processor.impl;

/**
 * Defines all the possible states for the Message Processor.
 * 
 * @author ravindra
 *
 */
public enum MessageProcessorState {
	/**
	 * This is used to represent an undefined state which we are NOT interested
	 * in. For an example before starting the message processor, we assume that
	 * it is in this undefined state.
	 */
	OTHER,
	/**
	 * The message processor is started/activated.
	 */
	STARTED,
	/**
	 * The message processor is paused.
	 */
	PAUSED,
	/**
	 * The message processor is deactivated.
	 */
	STOPPED,
	/**
	 * The message processor is destroyed/deleted.
	 */
	DESTROYED;

}
