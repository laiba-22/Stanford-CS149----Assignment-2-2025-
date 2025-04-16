#include "tasksys.h"
#include <thread>
#include <vector>
#include <algorithm>
#include <queue>
#include <atomic>
#include <mutex>
#include <condition_variable>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}



/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() 
{
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) 
{
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) 
{
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) 
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() 
{
    // You do not need to implement this method.
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



// CONSTRUCTOR - we've added a new parameter to store num of threads
TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), num_threads(num_threads) 
{
    
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) 
{
    
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.

    int num_threads_to_use = std::min(num_threads, num_total_tasks);    //how many threads to use
    
    std::vector<std::thread> workers;    //worker threads

    for (int i = 0; i < num_threads_to_use; ++i) 
    {
        workers.emplace_back([=]()
        {
            for (int task_id = i; task_id < num_total_tasks; task_id += num_threads_to_use) 
            {
                runnable->runTask(task_id, num_total_tasks);
            }
        });
    }

    for (auto& worker : workers) 
    {
        worker.join();
    }

}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
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


//CONSTRUCTOR - we make the thread pool here
TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) 
    : ITaskSystem(num_threads), num_threads(num_threads), tasks_remaining(0), stop(false)
{    
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.

    for (int i = 0; i < num_threads; ++i) 
    {
        workers.emplace_back([this]() 
        {
            while (!stop) 
            {
                int task_id = -1;
                //fetch a task if available
                {
                    std::lock_guard<std::mutex> lock(queue_mutex);
                    if (!task_queue.empty()) 
                    {
                        task_id = task_queue.front();
                        task_queue.pop();
                    }
                }
                //exec the task
                if (task_id != -1) 
                {
                    runnable->runTask(task_id, num_total_tasks);
                    tasks_remaining--;
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() 
{
    stop = true;
    for (auto& worker : workers) 
    {
        worker.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) 
{
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.

    this->runnable = runnable;
    this->num_total_tasks = num_total_tasks;

    tasks_remaining = num_total_tasks;

    //adding tasks to queue
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        for (int i = 0; i < num_total_tasks; ++i) 
        {
            task_queue.push(i);
        }
    }

    //busy wait until all tasks are done
    while (tasks_remaining > 0);

}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
: ITaskSystem(num_threads), num_threads(num_threads), stop(false), tasks_remaining(0) 
{
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    
    for (int i = 0; i < num_threads; ++i) 
    {
        workers.emplace_back([this]() 
        {
            while (true) 
            {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lock(queue_mutex);
                    condition.wait(lock, [this]() { return stop || !task_queue.empty(); });
                
                    if (stop && task_queue.empty()) return;
                
                    task = std::move(task_queue.front());
                    task_queue.pop();
                }
                task();
                //notify main thread 
                {
                    std::lock_guard<std::mutex> lock(queue_mutex);
                    tasks_remaining--;
                    if (tasks_remaining == 0) 
                    {
                        main_thread_condition.notify_one();
                    }
                }
            }
        });
    } 
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() 
{
    
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        stop = true;
    }
    condition.notify_all();

    for (auto& worker : workers) 
    {
        worker.join();
    }

}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) 
{

    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    
    this->runnable = runnable;
    this->num_total_tasks = num_total_tasks;
    tasks_remaining = num_total_tasks;
    
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        for (int i = 0; i < num_total_tasks; ++i) 
        {
            task_queue.push([=]() { runnable->runTask(i, num_total_tasks); });
        }
    }
    
    condition.notify_all();
    
    std::unique_lock<std::mutex> lock(queue_mutex);
    main_thread_condition.wait(lock, [this]() { return tasks_remaining == 0; });
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
