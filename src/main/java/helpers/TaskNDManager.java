package helpers;

import ai.djl.ndarray.NDManager;

/**
 * ND Manager per task
 * taskLifeCycleManager -> ND Manager that should be destroyed once the Task is finished
 * taskSingleProcessManager -> ND Manager that should be destroyed after each process
 * Implemented as a helper to manager memory issued by NDManager
 */
public class TaskNDManager {
    public final NDManager taskLifeCycleManager;
    public  NDManager taskSingleProcessManager;

    public TaskNDManager(){
        taskLifeCycleManager = NDManager.newBaseManager();
        taskSingleProcessManager = taskLifeCycleManager.newSubManager();
    }

    public  NDManager getTempManager(){
        return taskSingleProcessManager;
    }

    public  NDManager getGlobalManager(){
        return taskLifeCycleManager;
    }

    public void clean(){
        taskSingleProcessManager.close();
        taskSingleProcessManager = taskLifeCycleManager.newSubManager();
    }

    public  void close(){
        taskLifeCycleManager.close();
    }

}
