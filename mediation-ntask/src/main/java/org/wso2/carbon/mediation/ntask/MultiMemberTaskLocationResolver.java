package org.wso2.carbon.mediation.ntask;

import org.wso2.carbon.ntask.common.TaskException;
import org.wso2.carbon.ntask.core.TaskInfo;
import org.wso2.carbon.ntask.core.TaskLocationResolver;
import org.wso2.carbon.ntask.core.TaskServiceContext;

/**
 * Use this {@link TaskLocationResolver} only if you need to run the same task
 * on multiple worker nodes concurrently. For each worker/member where you need
 * to run the task concurrently, you may have to call this
 * {@link TaskLocationResolver} and get the member location index value. Then
 * you can schedule that particular task on the selected member node.
 * 
 * @author ravindra
 *
 */
public class MultiMemberTaskLocationResolver implements TaskLocationResolver {
	private static int counter;
	static {
		counter = 0;
	}

	@Override
	public int getLocation(TaskServiceContext ctx, TaskInfo taskInfo)
			throws TaskException {
		return counter++ % ctx.getServerCount();
	}
}
