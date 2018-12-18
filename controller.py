import numpy as np
import multiprocessing
from kalmanFilter import kalmanEstimator
from load_generator import load_process
from regression_utils import regularized_lin_regression
import json
import requests
import time
import math
from urllib.request import urlopen


# Some global variables go here
# Number of load generating processes to spawn
# number_of_processes = 5

nodes = {}
services = {}

def calculate_cpu_and_mem_percent(d):
    cpu_count = len(d["cpu_stats"]["cpu_usage"]["percpu_usage"])
    cpu_percent = 0.0
    cpu_delta = float(d["cpu_stats"]["cpu_usage"]["total_usage"]) - \
                float(d["precpu_stats"]["cpu_usage"]["total_usage"])
    system_delta = float(d["cpu_stats"]["system_cpu_usage"]) - \
                   float(d["precpu_stats"]["system_cpu_usage"])
    if system_delta > 0.0:
        cpu_percent = cpu_delta / system_delta * 100.0 * cpu_count
    mem_usage = float(d["memory_stats"]["usage"])
    mem_limit = float(d["memory_stats"]["limit"])
    mem_percentage = mem_usage / mem_limit * 100
    return cpu_percent, mem_percentage


def get_tasks(service, manager):
    service["tasks"] = []
    with urlopen(
            'http://{manager}/tasks?filters={{"service":{{"{service}":true}},"desired-state":{{"running":true}}}}'.format(
                manager=manager, service=service["name"])) as url:
        data = json.loads(url.read().decode())
        print("{service} tasks:".format(service=service["name"]))
        for task in data:
            if task["Status"]["State"] == "running":
                container_id = task["Status"]["ContainerStatus"]["ContainerID"]
            else:
                continue
            node_id = task["NodeID"]
            service["tasks"].append({"ContainerID": container_id, "NodeID": node_id})
            print('''\t ContainerID: {}, NodeID: {} '''.format(container_id, node_id))


# get all NodeIDs in swarm
# nodes = {}
# print("Nodes:")
# Get node IDs
def getNodeIDs(node_list, nodes):
    for node in node_list:
        with urlopen("http://{node}/info".format(node=node)) as url:
            data = json.loads(url.read().decode())
            nodes[data["Swarm"]["NodeID"]] = node
            print('''\t NodeID: {} '''.format(
                data["Swarm"]["NodeID"], ))


# list all the services
# services = {}
def getServices(services, manager):
    with urlopen("http://{manager}/services".format(manager=manager)) as url:
        data = json.loads(url.read().decode())
        print("Services:")
        for service in data:
            services[service["Spec"]["Name"]] = {"name": service["Spec"]["Name"], "tasks": []}
            print('''\t name: {}, version: {}, replicas: {}  '''.format(
                service["Spec"]["Name"],
                service["Version"]["Index"],
                service["Spec"]["Mode"]["Replicated"]["Replicas"]))


# get the tasks running on our swarm cluster

# for service_name, service in services.items():
# get_tasks(service)

def get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, sql_cpu_avg, sql_mem_avg, web_worker_cpu_avg, web_worker_mem_avg):
    for task in services["web-worker"]["tasks"]:
        with urlopen('http://{node}/containers/{containerID}/stats?stream=false'.format(
                node=nodes[task["NodeID"]], containerID=task["ContainerID"])) as url:
            data = json.loads(url.read().decode())
            task_cpu, task_mem = calculate_cpu_and_mem_percent(data)
            web_worker_cpu_usages.append(task_cpu)
            web_worker_mem_usages.append(task_mem)

    # repeat for sql stats
    for task in services["mysql"]["tasks"]:
        with urlopen('http://{node}/containers/{containerID}/stats?stream=false'.format(
                node=nodes[task["NodeID"]], containerID=task["ContainerID"])) as url:
            data = json.loads(url.read().decode())
            task_cpu, task_mem = calculate_cpu_and_mem_percent(data)
            sql_cpu_usages.append(task_cpu)
            sql_mem_usages.append(task_mem)
    # get averages
    sql_cpu_avg = sum(sql_cpu_usages) / len(sql_cpu_usages)
    sql_mem_avg = sum(sql_mem_usages) / len(sql_mem_usages)
    web_worker_cpu_avg = sum(web_worker_cpu_usages) / len(web_worker_cpu_usages)
    web_worker_mem_avg = sum(web_worker_mem_usages) / len(web_worker_mem_usages)


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


# def init_system(process_list, read_pipes, write_pipes, estimator, node_list, manager
def controller(input_pipe, output_pipe, number_of_processes, node_list, manager, polling_interval, polls_per_update):
    close_flag = False
    # Node list
    #node_list = ["192.168.56.102:4000", "192.168.56.103:4000", "192.168.56.101:4000"]
    #manager = "192.168.56.102:4000"
    services = {}

    # upper and lower cpu usage thresholds where scaling should happen on
    cpu_upper_threshold = 0.5
    cpu_lower_threshold = 0.2
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

        temp_process = multiprocessing.Process(target=load_process, args=(node_list, child_pipes[i]))
        process_list.append(temp_process)


    # get services, nodes and tasks
    getNodeIDs(node_list, nodes)
    getServices(services, manager)
    for service_name, service in services.items():
        get_tasks(service, manager)

    # get initial stats
    # get web-worker stats
    get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, sql_cpu_avg, sql_mem_avg, web_worker_cpu_avg, web_worker_mem_avg)

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
    store_stats(sql_cpu_avg, sql_mem_avg, web_worker_mem_avg, web_worker_cpu_avg, num_web_workers, num_sql,
                num_requests, prev_sql_cpu_avg, prev_sql_mem_avg, prev_web_worker_mem_avg, prev_web_worker_cpu_avg,
                prev_num_web_workers, prev_num_sql, prev_num_requests)
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
    print('BOOM {}'.format(num_requests))

    # get the stats
    get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, sql_cpu_avg, sql_mem_avg, web_worker_cpu_avg, web_worker_mem_avg)
    # create some np arrays for the regression
    sql_cpu_history = np.asarray(sql_cpu_avg - prev_sql_cpu_avg)
    sql_mem_history = np.asarray(sql_mem_avg - prev_sql_mem_avg)
    web_worker_cpu_history = np.asarray(web_worker_cpu_avg - prev_web_worker_cpu_avg)
    web_worker_mem_history = np.asarray(web_worker_mem_avg - prev_web_worker_mem_avg)
    request_history = np.asarray(num_requests - prev_num_requests)
    web_work_history = np.asarray(num_web_workers - prev_num_web_workers)
    sql_history = np.asarray(num_sql - prev_num_sql)
    # As before we store the stats
    store_stats(sql_cpu_avg, sql_mem_avg, web_worker_mem_avg, web_worker_cpu_avg, num_web_workers, num_sql,
                num_requests, prev_sql_cpu_avg, prev_sql_mem_avg, prev_web_worker_mem_avg, prev_web_worker_cpu_avg,
                prev_num_web_workers, prev_num_sql, prev_num_requests)
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
    get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, sql_cpu_avg,
              sql_mem_avg, web_worker_cpu_avg, web_worker_mem_avg)
    # Append new values to the histories
    sql_cpu_history = np.append(sql_cpu_history, sql_cpu_avg - prev_sql_cpu_avg)
    sql_mem_history = np.append(sql_mem_history, sql_mem_avg - prev_sql_mem_avg)
    web_worker_cpu_history = np.append(web_worker_cpu_history, web_worker_cpu_avg - prev_web_worker_cpu_avg)
    web_worker_mem_history = np.append(web_worker_mem_history, web_worker_mem_avg - prev_web_worker_mem_avg)
    request_history = np.append(request_history, num_requests - prev_num_requests)
    web_work_history = np.append(web_work_history, num_web_workers - prev_num_web_workers)
    sql_history = np.append(sql_history, num_sql - prev_num_sql)
    # Store the stats
    store_stats(sql_cpu_avg, sql_mem_avg, web_worker_mem_avg, web_worker_cpu_avg, num_web_workers, num_sql,
                num_requests, prev_sql_cpu_avg, prev_sql_mem_avg, prev_web_worker_mem_avg, prev_web_worker_cpu_avg,
                prev_num_web_workers, prev_num_sql, prev_num_requests)


    # ******************************************************************************************************************
    # ********************************************* 3rd DIFF MEASUREMENT ***********************************************

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
    get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, sql_cpu_avg, sql_mem_avg, web_worker_cpu_avg, web_worker_mem_avg)
    # Append new values to the histories
    sql_cpu_history = np.append(sql_cpu_history, sql_cpu_avg - prev_sql_cpu_avg)
    sql_mem_history = np.append(sql_mem_history, sql_mem_avg - prev_sql_mem_avg)
    web_worker_cpu_history = np.append(web_worker_cpu_history, web_worker_cpu_avg - prev_web_worker_cpu_avg)
    web_worker_mem_history = np.append(web_worker_mem_history, web_worker_mem_avg - prev_web_worker_mem_avg)
    request_history = np.append(request_history, num_requests - prev_num_requests)
    web_work_history = np.append(web_work_history, num_web_workers - prev_num_web_workers)
    sql_history = np.append(sql_history, num_sql - prev_num_sql)
    # Store the stats
    store_stats(sql_cpu_avg, sql_mem_avg, web_worker_mem_avg, web_worker_cpu_avg, num_web_workers, num_sql, num_requests, prev_sql_cpu_avg, prev_sql_mem_avg, prev_web_worker_mem_avg, prev_web_worker_cpu_avg, prev_num_web_workers, prev_num_sql, prev_num_requests)


    # ******************************************************************************************************************
    # ********************************************* 4th DIFF MEASUREMENT ***********************************************

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
    get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, sql_cpu_avg, sql_mem_avg, web_worker_cpu_avg, web_worker_mem_avg)
    # Append new values to the histories
    sql_cpu_history = np.append(sql_cpu_history, sql_cpu_avg - prev_sql_cpu_avg)
    sql_mem_history = np.append(sql_mem_history, sql_mem_avg - prev_sql_mem_avg)
    web_worker_cpu_history = np.append(web_worker_cpu_history, web_worker_cpu_avg - prev_web_worker_cpu_avg)
    web_worker_mem_history = np.append(web_worker_mem_history, web_worker_mem_avg - prev_web_worker_mem_avg)
    request_history = np.append(request_history, num_requests - prev_num_requests)
    web_work_history = np.append(web_work_history, num_web_workers - prev_num_web_workers)
    sql_history = np.append(sql_history, num_sql - prev_num_sql)
    # Store the stats
    store_stats(sql_cpu_avg, sql_mem_avg, web_worker_mem_avg, web_worker_cpu_avg, num_web_workers, num_sql,
                num_requests, prev_sql_cpu_avg, prev_sql_mem_avg, prev_web_worker_mem_avg, prev_web_worker_cpu_avg,
                prev_num_web_workers, prev_num_sql, prev_num_requests)


    # ******************************************************************************************************************
    # ********************************************* REGRESSION *********************************************************

    # Use these lines whenever we update the regression
    # TODO put this into a function
    target_mat = np.vstack([sql_cpu_history, web_worker_cpu_history, sql_mem_history, web_worker_mem_history]).T
    design_mat = np.vstack([sql_history, web_work_history, request_history]).T
    control_matrix = regularized_lin_regression(design_mat, target_mat, 0.0001)
    estimator.update_B(control_matrix.T)
    
    #Helper vars
    polls_since_update = 0
    processes_started = 2
    # TODO We have generated an initial estimate
    # Begin by starting up the rest of the load generators and then monitoring and adjust
    close_flag = False
    while not close_flag:
        if input_pipe.poll():
            message = input_pipe.recv()
            if message == "Quit":
                close_flag = True
                for i in range(0, processes_started):
                    par_pipes[i].send("close")
                    output_pipe.send("close")
                    process_list[i].join()
        if processes_started != number_of_processes:
            #We haven't started all of the load generators
            #So start another
            process_list[processes_started].start()
            processes_started = processes_started + 1
        #Sleep at the start since we need to sleep on first entry
        time.sleep(polling_interval)
        for i in range(0, processes_started):
            par_pipes[i].send("poll")
        pipes_ready = poll_pipes(par_pipes, processes_started)
        # reset number of requests
        num_requests = 0
        for i in range(0, processes_started):
            num_requests = num_requests + par_pipes[i].recv()
        #We've slept so poll
        get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, sql_cpu_avg, sql_mem_avg, web_worker_cpu_avg, web_worker_mem_avg)
        #Check to see if we need to update the estimator
        if polls_since_update == polls_per_update:
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
            polls_since_update = 0
        else:
            polls_since_update = polls_since_update + 1
        #TODO For Carl: Get Estimate from Estimator, make scaling decision, send values to logger
        #Send the values to the logger
        output_pipe.send([

        # do the experiment here