import numpy as np
import multiprocessing
from kalmanFilter import kalmanEstimator
from load_generator import load_process
from regression_utils import regularized_lin_regression
from logger import logger
import json
import requests
import time
import math
from urllib.request import urlopen
import datetime
from utils import get_stats, getNodeIDs, get_tasks, getServices
# Some global variables go here
# Number of load generating processes to spawn
# number_of_processes = 5

#nodes = {}
#services = {}
search_range = 2
max_containers = 20



def store_stats(sql_cpu_avg, sql_mem_avg, web_worker_mem_avg, web_worker_cpu_avg, num_web_workers, num_sql, num_requests, prev_sql_cpu_avg, prev_sql_mem_avg, prev_web_worker_mem_avg, prev_web_worker_cpu_avg, prev_num_web_workers, prev_num_sql, prev_num_requests):
    # Just a function to assign values
    # Used to reduce number of lines
    prev_sql_cpu_avg = sql_cpu_avg
    prev_sql_mem_avg = sql_mem_avg
    prev_web_worker_cpu_avg = web_worker_cpu_avg
    prev_web_worker_mem_avg = web_worker_mem_avg
    prev_num_requests = num_requests
    prev_num_sql = num_sql
    prev_num_web_workers = num_web_workers
    #return prev_sql_cpu_avg, prev_sql_mem_avg, 


def scale(service, replicas, manager):
    # scales the number of containers
    with urlopen("http://{manager}/services/{service}".format(manager=manager, service=service["name"])) as url:
        data = json.loads(url.read().decode())
        version = data["Version"]["Index"]

        # the whole spec object should be sent to the update API,
        # otherwise the missing values will be replaced by default values
        spec = data["Spec"]
        spec["Mode"]["Replicated"]["Replicas"] = replicas

        r = requests.post("http://{manager}/services/{service}/update?version={version}".format(manager=manager,
                                                                                                service=service["name"],
                                                                                                version=version),
                                                                                                data=json.dumps(spec))
        if r.status_code == 200:
            # get_tasks(service)
            pass
        else:
            print(r.reason, r.text)


def poll_pipes(pipe_list, number_to_poll):
    for j in range(0, number_to_poll):
        while not pipe_list[j].poll():
            pass
    return True

def find_min(estimator, num_requests, search_range, threshold):
    i = 0
    j = 0
    flip = False
    estimate = estimator.estimate(np.array(0, 0, num_requests))
    while not (estimate[0] < threshold and estimate[1] < threshold) and not (i > search_range and j > search_range):
        if flip:
            i = i + 1
            flip = not flip
        else:
            j = j + 1
            flip = not flip
        estimate = estimator.estimate(np.array(i, j, num_requests))
    return i, j

# def init_system(process_list, read_pipes, write_pipes, estimator, node_list, manager
def controller(input_pipe, number_of_processes, node_list, req_list, manager, polling_interval, polls_per_update, log_file, nodes, services):
    close_flag = False
    # Node list
    #node_list = ["192.168.56.102:4000", "192.168.56.103:4000", "192.168.56.101:4000"]
    #manager = "192.168.56.102:4000"
    #services = {}

    # upper and lower cpu usage thresholds where scaling should happen on
    cpu_upper_threshold = 50.0
    cpu_lower_threshold = 20.0
    # create list of processes and pipes
    process_list = []
    # pipes that main thread will read from and load threads will write to
    par_pipes = []
    # pipes that main thread will write to and load threads will read from
    child_pipes = []
    sql_cpu_usages = []
    sql_mem_usages = []
    web_worker_cpu_usages = []
    web_worker_mem_usages = []
    sql_cpu_avg = 0
    sql_mem_avg = 0
    web_worker_mem_avg = 0
    web_worker_cpu_avg = 0
    num_web_workers = 2
    num_sql = 1
    num_requests = 0
    # Storage variables
    prev_sql_cpu_avg = 0
    prev_sql_mem_avg = 0
    prev_web_worker_mem_avg = 0
    prev_web_worker_cpu_avg = 0
    prev_num_web_workers = 0
    prev_num_sql = 0
    prev_num_requests = 0

    # CREATE SPECIFIED NUMBER OF PROCESSES
    for i in range(0, number_of_processes):
        # Create new pipe
        par_pipe, child_pipe = multiprocessing.Pipe()

        par_pipes.append(par_pipe)
        child_pipes.append(child_pipe)

        temp_process = multiprocessing.Process(target=load_process, args=(req_list, child_pipes[i]))
        process_list.append(temp_process)


    # get services, nodes and tasks
    
    #Always start with 2 web worker and 1 sql
    scale(services["web-worker"], num_web_workers, manager)
    scale(services["mysql"], num_sql, manager)
    time.sleep(7)
    for service_name, service in services.items():
        get_tasks(service, manager)

    # get initial stats
    # get web-worker stats
    sql_cpu_avg, web_worker_cpu_avg, sql_mem_avg, web_worker_mem_avg = get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, nodes)

    # initalize estimator
    init_x = np.asarray((sql_cpu_avg, web_worker_cpu_avg, sql_mem_avg, web_worker_mem_avg))
    init_x = init_x.reshape(init_x.size, 1)
    estimator = kalmanEstimator(np.identity(4), np.random.random((4, 3)), init_x)

    # APPROACH:
    # We need at least 4 measurements to ensure that a solution can be found
    # 1st & 2nd containers will remain the same

    # ******************************************************************************************************************
    # ********************************************* 1st DIFF MEASUREMENT ***********************************************

    # store measurements
    prev_sql_cpu_avg = sql_cpu_avg
    prev_sql_mem_avg = sql_mem_avg
    prev_web_worker_cpu_avg = web_worker_cpu_avg
    prev_web_worker_mem_avg = web_worker_mem_avg
    prev_num_requests = num_requests
    prev_num_sql = num_sql
    prev_num_web_workers = num_web_workers
    # Start generating a load
    process_list[0].start()
    # Wait a couple seconds
    time.sleep(5)

    # Send poll request to the process we started
    par_pipes[0].send("poll")
    while not par_pipes[0].poll():
        pass
    # If the loop above has been broken then we can read the information from the pipe
    num_requests = par_pipes[0].recv()
    #print('BOOM {}'.format(num_requests))

    # get the stats
    sql_cpu_usages = []
    sql_mem_usages = []
    web_worker_cpu_usages = []
    web_worker_mem_usages = []
    sql_cpu_avg, web_worker_cpu_avg, sql_mem_avg, web_worker_mem_avg = get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, nodes)
    # create some np arrays for the regression
    sql_cpu_history = np.asarray(sql_cpu_avg - prev_sql_cpu_avg)
    sql_mem_history = np.asarray(sql_mem_avg - prev_sql_mem_avg)
    web_worker_cpu_history = np.asarray(web_worker_cpu_avg - prev_web_worker_cpu_avg)
    web_worker_mem_history = np.asarray(web_worker_mem_avg - prev_web_worker_mem_avg)
    request_history = np.asarray(num_requests - prev_num_requests)
    web_work_history = np.asarray(num_web_workers - prev_num_web_workers)
    sql_history = np.asarray(num_sql - prev_num_sql)
    # As before we store the stats
    prev_sql_cpu_avg = sql_cpu_avg
    prev_sql_mem_avg = sql_mem_avg
    prev_web_worker_cpu_avg = web_worker_cpu_avg
    prev_web_worker_mem_avg = web_worker_mem_avg
    prev_num_requests = num_requests
    prev_num_sql = num_sql
    prev_num_web_workers = num_web_workers
    # Wait a couple more seconds
    time.sleep(5)
    
    # ******************************************************************************************************************
    # ********************************************* 2nd DIFF MEASUREMENT ***********************************************

    # Send poll request to the process we started
    par_pipes[0].send("poll")
    while not par_pipes[0].poll():
        pass
    # If the loop above has been broken then we can read the information from the pipe
    num_requests = par_pipes[0].recv()
    # get the stats
    sql_cpu_usages = []
    sql_mem_usages = []
    web_worker_cpu_usages = []
    web_worker_mem_usages = []
    sql_cpu_avg, web_worker_cpu_avg, sql_mem_avg, web_worker_mem_avg = get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, nodes)
    # Append new values to the histories
    sql_cpu_history = np.append(sql_cpu_history, sql_cpu_avg - prev_sql_cpu_avg)
    sql_mem_history = np.append(sql_mem_history, sql_mem_avg - prev_sql_mem_avg)
    web_worker_cpu_history = np.append(web_worker_cpu_history, web_worker_cpu_avg - prev_web_worker_cpu_avg)
    web_worker_mem_history = np.append(web_worker_mem_history, web_worker_mem_avg - prev_web_worker_mem_avg)
    request_history = np.append(request_history, num_requests - prev_num_requests)
    web_work_history = np.append(web_work_history, num_web_workers - prev_num_web_workers)
    sql_history = np.append(sql_history, num_sql - prev_num_sql)
    print(web_worker_cpu_avg)
    # Store the stats
    prev_sql_cpu_avg = sql_cpu_avg
    prev_sql_mem_avg = sql_mem_avg
    prev_web_worker_cpu_avg = web_worker_cpu_avg
    prev_web_worker_mem_avg = web_worker_mem_avg
    prev_num_requests = num_requests
    prev_num_sql = num_sql
    prev_num_web_workers = num_web_workers

    
    print(web_worker_cpu_usages)
    # ******************************************************************************************************************
    # ********************************************* 3rd DIFF MEASUREMENT ***********************************************
    print("Two measurements taken\n")
    # Start 2 new containers
    num_web_workers = num_web_workers + 1
    num_sql = num_sql + 1
    scale(services["web-worker"], num_web_workers, manager)
    scale(services["mysql"], num_sql, manager)
    # We also start another load generator
    process_list[1].start()
    # as before we sleep and will update
    time.sleep(5)

    # poll pipes [0] & [1]
    for i in range(0, 2):
        par_pipes[i].send("poll")
    pipes_ready = poll_pipes(par_pipes, 2)
    # reset number of requests
    num_requests = 0
    for i in range(0, 2):
        num_requests = num_requests + par_pipes[i].recv()

    # update tasks since we scaled
    for service_name, service in services.items():
        get_tasks(service, manager)
    # get the stats
    sql_cpu_usages = []
    sql_mem_usages = []
    web_worker_cpu_usages = []
    web_worker_mem_usages = []
    sql_cpu_avg, web_worker_cpu_avg, sql_mem_avg, web_worker_mem_avg = get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, nodes)
    # Append new values to the histories
    sql_cpu_history = np.append(sql_cpu_history, sql_cpu_avg - prev_sql_cpu_avg)
    sql_mem_history = np.append(sql_mem_history, sql_mem_avg - prev_sql_mem_avg)
    web_worker_cpu_history = np.append(web_worker_cpu_history, web_worker_cpu_avg - prev_web_worker_cpu_avg)
    web_worker_mem_history = np.append(web_worker_mem_history, web_worker_mem_avg - prev_web_worker_mem_avg)
    request_history = np.append(request_history, num_requests - prev_num_requests)
    web_work_history = np.append(web_work_history, num_web_workers - prev_num_web_workers)
    sql_history = np.append(sql_history, num_sql - prev_num_sql)
    # Store the stats
    prev_sql_cpu_avg = sql_cpu_avg
    prev_sql_mem_avg = sql_mem_avg
    prev_web_worker_cpu_avg = web_worker_cpu_avg
    prev_web_worker_mem_avg = web_worker_mem_avg
    prev_num_requests = num_requests
    prev_num_sql = num_sql
    prev_num_web_workers = num_web_workers


    # ******************************************************************************************************************
    # ********************************************* 4th DIFF MEASUREMENT ***********************************************
    print("3 measurements taken\n")
    # Now we get the 4th measurement
    # Scale down the number of sql containers and scale up web-worker
    num_sql = num_sql - 1
    num_web_workers = num_web_workers + 1
    scale(services["web-worker"], num_web_workers, manager)
    scale(services["mysql"], num_sql, manager)
    # as before we sleep and will update
    time.sleep(5)
    for service_name, service in services.items():
        get_tasks(service, manager)
    for i in range(0, 2):
        par_pipes[i].send("poll")
    pipes_ready = poll_pipes(par_pipes, 2)
    # reset number of requests
    num_requests = 0
    for i in range(0, 2):
        num_requests = num_requests + par_pipes[i].recv()

    # get the stats
    sql_cpu_usages = []
    sql_mem_usages = []
    web_worker_cpu_usages = []
    web_worker_mem_usages = []
    sql_cpu_avg, web_worker_cpu_avg, sql_mem_avg, web_worker_mem_avg = get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, nodes)
    
    # Append new values to the histories
    sql_cpu_history = np.append(sql_cpu_history, sql_cpu_avg - prev_sql_cpu_avg)
    sql_mem_history = np.append(sql_mem_history, sql_mem_avg - prev_sql_mem_avg)
    web_worker_cpu_history = np.append(web_worker_cpu_history, web_worker_cpu_avg - prev_web_worker_cpu_avg)
    web_worker_mem_history = np.append(web_worker_mem_history, web_worker_mem_avg - prev_web_worker_mem_avg)
    request_history = np.append(request_history, num_requests - prev_num_requests)
    web_work_history = np.append(web_work_history, num_web_workers - prev_num_web_workers)
    sql_history = np.append(sql_history, num_sql - prev_num_sql)
    # Store the stats
    prev_sql_cpu_avg = sql_cpu_avg
    prev_sql_mem_avg = sql_mem_avg
    prev_web_worker_cpu_avg = web_worker_cpu_avg
    prev_web_worker_mem_avg = web_worker_mem_avg
    prev_num_requests = num_requests
    prev_num_sql = num_sql
    prev_num_web_workers = num_web_workers


    # ******************************************************************************************************************
    # ********************************************* REGRESSION *********************************************************

    # Use these lines whenever we update the regression
    # TODO put this into a function
    target_mat = np.vstack([sql_cpu_history, web_worker_cpu_history, sql_mem_history, web_worker_mem_history]).T
    design_mat = np.vstack([sql_history, web_work_history, request_history]).T
    control_matrix = regularized_lin_regression(design_mat, target_mat, 0.0001)
    #print(control_matrix)
    estimator.update_B(control_matrix.T)
    #print(control_matrix.T)
    obs = np.array([[sql_cpu_avg, web_worker_cpu_avg, sql_mem_avg, web_worker_mem_avg]]).T
    estimator.update(obs, np.identity(4))
    #Helper vars
    polls_since_update = 0
    processes_started = 2
    delta_web = 0
    delta_sql = 0
    delta_requests = 0
    scaling_triggered = False
    # TODO We have generated an initial estimate
    # Begin by starting up the rest of the load generators and then monitoring and adjust
    close_flag = False
    #print("Experiment Started\n")
    output_pipe, log_pipe = multiprocessing.Pipe()
    close_pipe, log_close_pipe = multiprocessing.Pipe()
    startTime = time.time()
    log_process = multiprocessing.Process(target=logger, args=(log_pipe, log_file, node_list, manager, startTime, polling_interval/4.0, nodes, services, log_close_pipe))
    log_process.start()
    iteration_count = 0
    #old_time = datetime.datetime.now()
    output_pipe.send([estimator.x[0][0],estimator.x[1][0], estimator.x[2][0], estimator.x[3][0], num_sql, num_web_workers, delta_requests, iteration_count, 0.0, 0.0, True])
    print("Experiment Started")
    while not close_flag:
        #old_time = time.time()
        if input_pipe.poll():
            message = input_pipe.recv()
            if message == "Quit":
                close_flag = True
                print("Shutting down")
                for i in range(0, processes_started):
                    par_pipes[i].send("close")
                    
                    process_list[i].join()
                    print("Load process {0}".format(i))
                print("Loads spun down")
                scale(services["web-worker"], 2, manager)
                scale(services["mysql"], 1, manager)
                output_pipe.send("close")
                close_pipe.send("close")
                log_process.join()
                print("Logger shut down")
                break
        if (processes_started != number_of_processes):
            #We haven't started all of the load generators
            #So start another
            process_list[processes_started].start()
            processes_started = processes_started + 1
        #Sleep at the start since we need to sleep on first entry
        
        time.sleep(polling_interval)
        if scaling_triggered:
            for service_name, service in services.items():
                get_tasks(service, manager)
            
            diff_time = time.time() - startTime
            minutes, seconds = diff_time // 60, diff_time % 60
            output_pipe.send([estimator.x[0][0],estimator.x[1][0], estimator.x[2][0], estimator.x[3][0], num_sql, num_web_workers, delta_requests, iteration_count, minutes, seconds, scaling_triggered])
            scaling_triggered = False
        iteration_count = iteration_count + 1
        for i in range(0, processes_started):
            par_pipes[i].send("poll")
        pipes_ready = poll_pipes(par_pipes, processes_started)
        # reset number of requests
        num_requests = 0
        for i in range(0, processes_started):
            num_requests = num_requests + par_pipes[i].recv()
        delta_requests = num_requests - prev_num_requests
        #We've slept so poll
        sql_cpu_usages = []
        sql_mem_usages = []
        web_worker_cpu_usages = []
        web_worker_mem_usages = []
        
        #Check to see if we need to update the estimator
        if polls_since_update == polls_per_update:
            sql_cpu_avg, web_worker_cpu_avg, sql_mem_avg, web_worker_mem_avg = get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, nodes)
            #need to update the estimator
            #Check to see if we have 100 entries in the history list
            if sql_cpu_history.size == 100:
                #We have 100 entries randomly replace one of them
                replacement_index = random.randint(0, 99)
                #Use np.put to insert new value at index replacement_index, overwriting previous value
                np.put(sql_cpu_history, replacement_index, sql_cpu_avg - prev_sql_cpu_avg)
                np.put(sql_mem_history, replacement_index, sql_mem_avg - prev_sql_mem_avg)
                np.put(web_worker_cpu_history, replacement_index, web_worker_cpu_avg - prev_web_worker_cpu_avg)
                np.put(web_worker_mem_history, replacement_index, web_worker_mem_avg - prev_web_worker_mem_avg)
                np.put(request_history, replacement_index, num_requests - prev_num_requests)
                np.put(web_work_history, replacement_index, num_web_workers - prev_num_web_workers)
                np.put(sql_history, replacement_index, num_sql - prev_num_sql)
                
            else:
                #Don't have 100 entries. Append new values
                sql_cpu_history = np.append(sql_cpu_history, sql_cpu_avg - prev_sql_cpu_avg)
                sql_mem_history = np.append(sql_mem_history, sql_mem_avg - prev_sql_mem_avg)
                web_worker_cpu_history = np.append(web_worker_cpu_history, web_worker_cpu_avg - prev_web_worker_cpu_avg)
                web_worker_mem_history = np.append(web_worker_mem_history, web_worker_mem_avg - prev_web_worker_mem_avg)
                request_history = np.append(request_history, num_requests - prev_num_requests)
                web_work_history = np.append(web_work_history, num_web_workers - prev_num_web_workers)
                sql_history = np.append(sql_history, num_sql - prev_num_sql)
            #Do regression
            target_mat = np.vstack([sql_cpu_history, web_worker_cpu_history, sql_mem_history, web_worker_mem_history]).T
            design_mat = np.vstack([sql_history, web_work_history, request_history]).T
            control_matrix = regularized_lin_regression(design_mat, target_mat, 0.0001)
            estimator.update_B(control_matrix.T)
            #Also need to correct Kalman gain
            estimator.update(np.array([[sql_cpu_avg, web_worker_cpu_avg, sql_mem_avg, web_worker_mem_avg]]).T, 0.002 * np.random.randn(4, 4))
            polls_since_update = 0
        else:
            polls_since_update = polls_since_update + 1
        #TODO For Carl: Get Estimate from Estimator, make scaling decision, send values to logger
        prev_sql_cpu_avg = sql_cpu_avg
        prev_sql_mem_avg = sql_mem_avg
        prev_web_worker_cpu_avg = web_worker_cpu_avg
        prev_web_worker_mem_avg = web_worker_mem_avg
        prev_num_requests = num_requests
        prev_num_sql = num_sql
        prev_num_web_workers = num_web_workers
        estimate = estimator.estimate(np.array([[0, 0, delta_requests]]).T)
        #print(estimate)
        if (estimate[1] >= cpu_upper_threshold):
            #We assume the web worker needs scaling most of the time
            
            while not (estimate[1] < cpu_upper_threshold or delta_web == search_range or num_web_workers + (delta_web + 1) > max_containers):
                delta_web = delta_web + 1
                estimate = estimator.estimate(np.array([[0, delta_web, delta_requests]]).T)
                scaling_triggered = True

        if (estimate[0] >= cpu_upper_threshold):
            
            while not (estimate[0] < cpu_upper_threshold or delta_sql == search_range or num_sql + (delta_sql + 1) > max_containers):
                delta_sql = delta_sql + 1
                estimate = estimator.estimate(np.array([[delta_sql, delta_web, delta_requests]]).T)
                scaling_triggered = True
        if not scaling_triggered:
            #just to prevent two cases triggering
            if estimate[1] <= cpu_lower_threshold:
                
                while not (estimate[1] > cpu_lower_threshold or abs(delta_web) == search_range or num_web_workers + (delta_web - 1) < 1):
                    delta_web = delta_web - 1
                    estimate = estimator.estimate(np.array([[0, delta_web, delta_requests]]).T)
                    #We assume the web worker needs scaling most of the time
                    scaling_triggered = True

            if (estimate[0] <= cpu_lower_threshold):
                
                while not (estimate[0] > cpu_lower_threshold or abs(delta_sql) == search_range or num_sql + (delta_sql-1) < 1):
                    delta_sql = delta_sql - 1
                    estimate = estimator.estimate(np.array([[delta_sql, delta_web, delta_requests]]).T)
                    scaling_triggered = True
        #We have made our decision actually update estimator
        estimator.predict(np.array([[delta_sql, delta_web, delta_requests]]).T)
        if scaling_triggered:
            #Actually do the scaling here
            num_web_workers = num_web_workers + delta_web
            num_sql = num_sql + delta_sql
            scale(services["web-worker"], num_web_workers, manager)
            scale(services["mysql"], num_sql, manager)
            delta_web = 0
            delta_sql = 0
            #scaling_triggered = 0
            #time.sleep(0.05)
            
        #Send the values to the logger
        #order will be sql_cpu web_worker_cpu sql_mem web_worker_mem num_sql num_web_workers
        #For each value we send actual then predicted
        if not scaling_triggered:
            diff_time = time.time() - startTime
            minutes, seconds = diff_time // 60, diff_time % 60
            output_pipe.send([estimator.x[0][0],estimator.x[1][0], estimator.x[2][0], estimator.x[3][0], num_sql, num_web_workers, delta_requests, iteration_count, minutes, seconds, scaling_triggered])
        #time.sleep(polling_interval)
        # do the experiment here