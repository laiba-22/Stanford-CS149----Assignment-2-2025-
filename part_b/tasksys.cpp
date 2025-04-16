#include "tasksys.h"
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <functional>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}


/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads) {}
TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
    return 0;
}

void TaskSystemSerial::sync() {
    return;
}


/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

 const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}



/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

 const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}







/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() 
{
    return "Parallel + Thread Pool + Sleep";
}

//constructor
TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads), num_threads(num_threads), stop(false) 
{

    for (int i = 0; i < num_threads; ++i) 
    {
        workers.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerThread, this);
    }
}


TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() 
{
    {
        std::lock_guard<std::mutex> lock(queueMutex);
        stop = true;
    }
    taskAvailable.notify_all();

    for (auto& worker : workers) 
    {
        worker.join();
    }
}


void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) 
{
    
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();

}

// asynchronous with dependencies
int TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) 
{
    
    std::unique_lock<std::mutex> lock(depMutex);
    int taskID = nextTaskID++;  
    activeTasks += num_total_tasks;

    if (deps.empty()) 
    {
        //no dep so exec immediately
        for (int i = 0; i < num_total_tasks; ++i) 
        {
            {
                std::lock_guard<std::mutex> qLock(queueMutex);
                taskQueue.push([runnable, i, num_total_tasks, this]() 
                {  

                    runnable->runTask(i, num_total_tasks);  
                
                    std::unique_lock<std::mutex> depLock(depMutex);
                    if (--activeTasks == 0) 
                    {
                        allTasksCompleted.notify_all();
                    }
                });
            }
            taskAvailable.notify_one();
        }
    } 
    else 
    {
        //dep exists
        taskDependencies[taskID] = deps.size();
        dependentTasks[taskID] = {};

        for (int depID : deps) 
        {
            dependentTasks[depID].push_back([runnable, num_total_tasks, this, taskID]() 
            {

                std::unique_lock<std::mutex> depLock(depMutex);
                
                if (--taskDependencies[taskID] == 0) 
                {
                    for (int i = 0; i < num_total_tasks; ++i) 
                    {
                        {
                            std::lock_guard<std::mutex> qLock(queueMutex);
                            taskQueue.push([runnable, i, num_total_tasks, this]() 
                            {
                                runnable->runTask(i, num_total_tasks);  
                                         
                                std::unique_lock<std::mutex> depLock(depMutex);
                                if (--activeTasks == 0) 
                                {
                                    allTasksCompleted.notify_all();
                                }
                            });
                        }
                        taskAvailable.notify_one();
                    }
                }
            });
        }
    }
    
    return taskID;
}


void TaskSystemParallelThreadPoolSleeping::sync() 
{
    std::unique_lock<std::mutex> lock(depMutex);
    allTasksCompleted.wait(lock, [this]() { return activeTasks == 0; });
}


void TaskSystemParallelThreadPoolSleeping::workerThread() 
{
    while (true) 
    {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            taskAvailable.wait(lock, [this]() { return !taskQueue.empty() || stop; });
            if (stop && taskQueue.empty()) return;
            task = std::move(taskQueue.front());
            taskQueue.pop();
        }
        task();
    }
}