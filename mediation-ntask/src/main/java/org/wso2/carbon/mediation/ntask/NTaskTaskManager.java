package org.wso2.carbon.mediation.ntask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.task.SynapseTaskException;
import org.apache.synapse.task.TaskDescription;
import org.apache.synapse.task.TaskManager;
import org.wso2.carbon.base.MultitenantConstants;
import org.wso2.carbon.context.CarbonContext;
import org.wso2.carbon.mediation.ntask.internal.NtaskService;
import org.wso2.carbon.ntask.common.TaskException;
import org.wso2.carbon.ntask.core.TaskInfo;
import org.wso2.carbon.ntask.core.service.TaskService;
import org.wso2.carbon.utils.CarbonUtils;

public class NTaskTaskManager implements TaskManager, TaskServiceObserver {
	private static final Object lock = new Object();

	private static final Log logger = LogFactory.getLog(NTaskTaskManager.class
			.getName());

	private String name;

	private boolean initialized = false;

	private org.wso2.carbon.ntask.core.TaskManager taskManager;

	private Map<String, Object> properties = new HashMap<String, Object>(5);

	private List<TaskInfo> pendingTasks = new ArrayList<TaskInfo>();

	protected final Properties configProperties = new Properties();

	public boolean schedule(TaskDescription taskDescription) {
		logger.debug("#schedule Scheduling task:" + taskDescription.getName());
		TaskInfo taskInfo;
		try {
			taskInfo = TaskBuilder.buildTaskInfo(taskDescription, properties);
		} catch (Exception e) {
			if (logger.isDebugEnabled()) {
				logger.debug(
						"#schedule Could not build task info object. Error:"
								+ e.getLocalizedMessage(), e);
			}
			return false;
		}
		if (!isInitialized()) {
			// if cannot schedule yet, put in the pending tasks list.
			synchronized (lock) {
				logger.debug("#schedule Added pending task ["
						+ taskInfo.getName() + "]");
				pendingTasks.add(taskInfo);
			}
			return false;
		}
		try {
			synchronized (lock) {
				taskManager.registerTask(taskInfo);
				taskManager.scheduleTask(taskInfo.getName());
			}
			logger.info("Scheduled task [" + taskDescription.getName() + "::"
					+ taskDescription.getTaskGroup() + "]");
		} catch (Exception e) {
			logger.error(
					"Scheduling task [" + taskDescription.getName() + "::"
							+ taskDescription.getTaskGroup()
							+ "] FAILED. Error: " + e.getLocalizedMessage(), e);
			return false;
		}
		return true;
	}

	public boolean reschedule(String taskName, TaskDescription taskDescription) {
		if (!isInitialized()) {
			return false;
		}
		try {
			synchronized (lock) {
				TaskInfo taskInfo = taskManager.getTask(taskName);
				TaskDescription description = TaskBuilder
						.buildTaskDescription(taskInfo);
				taskInfo = TaskBuilder.buildTaskInfo(description, properties);
				taskManager.registerTask(taskInfo);
				taskManager.rescheduleTask(taskInfo.getName());
			}
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	public boolean delete(String taskName) {
		if (!isInitialized()) {
			return false;
		}
		if (taskName == null) {
			return false;
		}
		String list[] = taskName.split("::");
		String name = list[0];
		if (name == null || "".equals(name)) {
			throw new SynapseTaskException("Task name is null. ", logger);
		}
		String group = list[1];
		if (group == null || "".equals(group)) {
			group = TaskDescription.DEFAULT_GROUP;
			if (logger.isDebugEnabled()) {
				logger.debug("Task group is null or empty , using default group :"
						+ TaskDescription.DEFAULT_GROUP);
			}
		}
		try {
			boolean deleted;
			synchronized (lock) {
				deleted = taskManager.deleteTask(name);
			}
			if (deleted) {
				NTaskAdapter.removeProperty(taskName);
			}
			logger.debug("Deleted task [" + name + "] [" + deleted + "]");
			return deleted;
		} catch (Exception e) {
			logger.error("Cannot delete task [" + taskName + "::" + group
					+ "]. Error: " + e.getLocalizedMessage(), e);
			return false;
		}
	}

	public boolean pause(String taskName) {
		if (!isInitialized()) {
			return false;
		}
		try {
			synchronized (lock) {
				taskManager.pauseTask(taskName);
			}
			return true;
		} catch (Exception e) {
			logger.error(
					"Cannot pause task [" + taskName + "]. Error: "
							+ e.getLocalizedMessage(), e);
		}
		return false;
	}

	public boolean pauseAll() {
		if (!isInitialized()) {
			return false;
		}
		try {
			synchronized (lock) {
				List<TaskInfo> taskList = taskManager.getAllTasks();
				for (TaskInfo taskInfo : taskList) {
					taskManager.pauseTask(taskInfo.getName());
				}
			}
			return true;
		} catch (Exception e) {
			logger.error(
					"Cannot pause all tasks. Error: " + e.getLocalizedMessage(),
					e);
		}
		return false;
	}

	public boolean resume(String taskName) {
		if (!isInitialized()) {
			return false;
		}
		if (taskName == null) {
			return false;
		}
		try {
			synchronized (lock) {
				taskManager.resumeTask(taskName);
			}
		} catch (Exception e) {
			logger.error(
					"Cannot resume task [" + taskName + "]. Error: "
							+ e.getLocalizedMessage(), e);
			return false;
		}
		return true;
	}

	public boolean resumeAll() {
		if (!isInitialized()) {
			return false;
		}
		try {
			synchronized (lock) {
				List<TaskInfo> taskList = taskManager.getAllTasks();
				for (TaskInfo taskInfo : taskList) {
					taskManager.resumeTask(taskInfo.getName());
				}
			}
			return true;
		} catch (Exception e) {
			logger.error(
					"Cannot resume all tasks. Error: "
							+ e.getLocalizedMessage(), e);
		}
		return false;
	}

	public TaskDescription getTask(String taskName) {
		if (!isInitialized()) {
			return null;
		}
		try {
			TaskInfo taskInfo;
			synchronized (lock) {
				taskInfo = taskManager.getTask(taskName);
			}
			return TaskBuilder.buildTaskDescription(taskInfo);
		} catch (Exception e) {
			logger.error(
					"Cannot return task [" + taskName + "]. Error: "
							+ e.getLocalizedMessage(), e);
			return null;
		}
	}

	public String[] getTaskNames() {
		if (!isInitialized()) {
			return new String[0];
		}
		try {
			List<TaskInfo> taskList;
			synchronized (lock) {
				taskList = taskManager.getAllTasks();
			}
			List<String> result = new ArrayList<String>();
			for (TaskInfo taskInfo : taskList) {
				result.add(taskInfo.getName());
			}
			return result.toArray(new String[result.size()]);
		} catch (Exception e) {
			logger.error(
					"Cannot return task list. Error: "
							+ e.getLocalizedMessage(), e);
		}
		return new String[0];
	}

	public boolean init(Properties properties) {
		synchronized (lock) {
			try {
				TaskService taskService = NtaskService.getTaskService();
				if (taskService == null
						|| NtaskService.getCcServiceInstance() == null) {
					// Cannot proceed with the initialization because the
					// TaskService is not yet
					// available. Register this as an observer so that this can
					// be reinitialized
					// within the NtaskService when the TaskService is
					// available.
					NtaskService.addObserver(this);
					return false;
				}
				boolean isSingleNode = NtaskService.getCcServiceInstance()
						.getServerConfigContext().getAxisConfiguration()
						.getClusteringAgent() == null;
				boolean isWorkerNode = !isSingleNode
						&& CarbonUtils.isWorkerNode();
				logger.debug("#init Single-Node: " + isSingleNode
						+ " Worker-Node: " + isWorkerNode);
				if (!isSingleNode && !isWorkerNode) {
					// Skip running tasks on the management node
					logger.debug("#init Skipping task registration");
					initialized = true;
					return true;
				}

				if ((taskManager = getTaskManager(false)) == null) {
					return false;
				}

				// Register pending tasks..
				Iterator tasks = pendingTasks.iterator();
				while (tasks.hasNext()) {
					TaskInfo taskInfo = (TaskInfo) tasks.next();
					try {
						List<TaskInfo> taskInfos = taskManager.getAllTasks();
						boolean hasTask = false;
						for (TaskInfo task : taskInfos) {
							if (task.getName().equals(taskInfo.getName())) {
								hasTask = true;
								break;
							}
						}
						if (hasTask) {
							logger.debug("#init Pending task ["
									+ taskInfo.getName()
									+ "] is already available in the registry.");
							continue;
						}
						taskManager.registerTask(taskInfo);
						taskManager.scheduleTask(taskInfo.getName());
						tasks.remove();
						logger.debug("#init Scheduled pending task ["
								+ taskInfo.getName() + "] SUCCESSFUL.");
					} catch (TaskException e) {
						logger.error(
								"Could not schedule task ["
										+ taskInfo.getName() + "]. Error: "
										+ e.getLocalizedMessage(), e);
					}
				}
				// Run already deployed tasks..
				taskService.registerTaskType(TaskBuilder.TASK_TYPE_USER);
				taskService.registerTaskType(TaskBuilder.TASK_TYPE_SYSTEM);
				initialized = true;
				logger.info("initialized");
				return true;
			} catch (Exception e) {
				logger.error(
						"Cannot initialize task manager. Error: "
								+ e.getLocalizedMessage(), e);
				initialized = false;
			}
		}
		return false;
	}

	public boolean update(Map<String, Object> parameters) {
		return init(parameters == null
				|| !parameters.containsKey("init.properties") ? null
				: (Properties) parameters.get("init.properties"));
	}

	public boolean isInitialized() {
		synchronized (lock) {
			return initialized;
		}
	}

	public boolean start() {
		return isInitialized();
	}

	public boolean stop() {
		// Nothing to do here.
		return true;
	}

	public int getRunningTaskCount() {
		if (!isInitialized()) {
			return -1;
		}
		String[] names = getTaskNames();
		int count = 0;
		try {
			for (String name : names) {
				synchronized (lock) {
					if (taskManager
							.getTaskState(name)
							.equals(org.wso2.carbon.ntask.core.TaskManager.TaskState.NORMAL)) {
						++count;
					}
				}
			}
		} catch (Exception e) {
			logger.error(
					"Cannot return running task count. Error: "
							+ e.getLocalizedMessage(), e);
		}
		return count;
	}

	public boolean isTaskRunning(Object o) {
		if (!isInitialized()) {
			return false;
		}
		String taskName;
		if (o instanceof String) {
			taskName = (String) o;
		} else {
			return false;
		}
		synchronized (lock) {
			try {
				return taskManager
						.getTaskState(taskName)
						.equals(org.wso2.carbon.ntask.core.TaskManager.TaskState.NORMAL);
			} catch (Exception e) {
				logger.error("Cannot return task status [" + taskName
						+ "]. Error: " + e.getLocalizedMessage(), e);
			}
		}
		return false;
	}

	public boolean setProperties(Map<String, Object> properties) {
		if (properties == null) {
			return false;
		}
		for (String key : properties.keySet()) {
			synchronized (lock) {
				this.properties.put(key, properties.get(key));
			}
		}
		return true;
	}

	public boolean setProperty(String name, Object property) {
		if (name == null) {
			return false;
		}
		synchronized (lock) {
			properties.put(name, property);
		}
		return true;
	}

	public Object getProperty(String name) {
		if (name == null) {
			return null;
		}
		synchronized (lock) {
			return properties.get(name);
		}
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public String getProviderClass() {
		return this.getClass().getName();
	}

	public Properties getConfigurationProperties() {
		synchronized (lock) {
			return configProperties;
		}
	}

	public void setConfigurationProperties(Properties properties) {
		if (properties == null) {
			return;
		}
		synchronized (lock) {
			configProperties.putAll(properties);
		}
	}

	private org.wso2.carbon.ntask.core.TaskManager getTaskManager(boolean system)
			throws Exception {
		TaskService taskService = NtaskService.getTaskService();
		if (taskService == null) {
			return null;
		}
		// TODO return our own task manager instance which wraps the Clustered
		// task manager here.
		return taskService.getTaskManager(system ? TaskBuilder.TASK_TYPE_SYSTEM
				: TaskBuilder.TASK_TYPE_USER);
	}

	private int getCurrentTenantId() {
		return CarbonContext.getThreadLocalCarbonContext().getTenantId();
	}

	private void checkSystemRequest() throws Exception {
		if (this.getCurrentTenantId() != MultitenantConstants.SUPER_TENANT_ID) {
			throw new Exception("System request verification failed, "
					+ "only Super-Tenant can make this type of requests");
		}
	}
}
