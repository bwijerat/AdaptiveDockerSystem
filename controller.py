import numpy as np
import multiprocessing
from kalmanFilter import kalmanEstimator
from load_generator import load_process
from regression_utils import regregularized_lin_regression
import json
import requests
import time
import math
from urllib.request import urlopen

#Some global variables go here
#Number of load generating processes to spawn
#number_of_processes = 5




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
    
    
def get_tasks(service):
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
#nodes = {}
#print("Nodes:")
#Get node IDs
def getNodeIDs(node_list, nodes)
    for node in node_list:
        with urlopen("http://{node}/info".format(node=node)) as url:
            data = json.loads(url.read().decode())
            nodes[data["Swarm"]["NodeID"]] = node
            print('''\t NodeID: {} '''.format(
                data["Swarm"]["NodeID"], ))

# list all the services
#services = {}
def getServices(services, manager)
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

#for service_name, service in services.items():
    #get_tasks(service)
    
def get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, sql_cpu_avg, sql_mem_avg, web_worker_cpu_avg, web_worker_mem_avg):
    for task in services["web-worker"]["tasks"]:
        with urlopen('http://{node}/containers/{containerID}/stats?stream=false'.format(
                node=nodes[task["NodeID"]], containerID=task["ContainerID"])) as url:
            data = json.loads(url.read().decode())
            task_cpu, task_mem = calculate_cpu_and_mem_percent(data)
            web_worker_cpu_usages.append(task_cpu)
            web_worker_mem_usages.append(task_mem)
    
    #repeat for sql stats
    for task in services["mysql"]["tasks"]:
        with urlopen('http://{node}/containers/{containerID}/stats?stream=false'.format(
                node=nodes[task["NodeID"]], containerID=task["ContainerID"])) as url:
            data = json.loads(url.read().decode())
            task_cpu, task_mem = calculate_cpu_and_mem_percent(data)
            sql_cpu_usages.append(task_cpu)
            sql_mem_usages.append(task_mem)
    #get averages
    sql_cpu_avg = sum(sql_cpu_usages) / len(sql_cpu_usages)
    sql_mem_avg = sum(sql_mem_usages) / len(sql_mem_usages)
    web_worker_cpu_avg = sum(web_worker_cpu_usages) / len(web_worker_cpu_usages)
    web_worker_mem_avg = sum(web_worker_mem_usages) / len(web_worker_mem_usages)
    
def store_stats(sql_cpu_avg, sql_mem_avg, web_worker_mem_avg, web_worker_cpu_avg, num_web_workers, num_sql, num_requests, prev_sql_cpu_avg, prev_sql_mem_avg, prev_web_worker_mem_avg, prev_web_worker_cpu_avg, prev_num_web_workers, prev_num_sql, prev_num_requests):
    #Just a function to assign values
    #Used to reduce number of lines
    prev_sql_cpu_avg = sql_cpu_avg
    prev_sql_mem_avg = sql_mem_avg
    prev_web_worker_cpu_avg = web_worker_cpu_avg
    prev_web_worker_mem_avg = web_worker_mem_avg
    prev_num_requests = num_requests
    prev_num_sql = num_sql
    prev_num_web_workers = num_web_workers

    
def scale(service, replicas, manager):
    #scales the number of containers
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
            #get_tasks(service)
        else:
            print(r.reason, r.text)

def poll_pipes(pipe_list, number_to_poll):
    for j in range(0,number_to_poll):
        while not pipe_list[j].poll():
            pass
    return True
        
#def init_system(process_list, read_pipes, write_pipes, estimator, node_list, manager            
def controller(input_pipe, output_pipe, number_of_processes):
    close_flag = False
    #Node list
    node_list = ["192.168.56.102:4000", "192.168.56.103:4000", "192.168.56.101:4000"]
    manager = "192.168.56.102:4000"

    # upper and lower cpu usage thresholds where scaling should happen on
    cpu_upper_threshold = 0.5
    cpu_lower_threshold = 0.2
    #create list of processes and pipes
    process_list = []
    #pipes that main thread will read from and load threads will write to
    read_pipes = []
    #pipes that main thread will write to and load threads will read from
    write_pipes = []
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
    #Storage variables
    prev_sql_cpu_avg = 0
    prev_sql_mem_avg = 0
    prev_web_worker_mem_avg = 0
    prev_web_worker_cpu_avg = 0
    prev_num_web_workers = 0
    prev_num_sql = 0
    prev_num_requests = 0
    for i in range(0, number_of_processes):
        #Create new pipe
        temp_read, temp_write = multiprocessing.Pipe()
        temp_process = multiprocessing.Process(target=load_process, args(node_list, temp_read, temp_write))
        process_list.append(temp_process)
        read_pipes.append(temp_read)
        write_pipes.append(temp_write)
    
    nodes = {}
    services = {}
    #get services, nodes and tasks
    getNodeIDs(node_list, nodes)
    getServices(services, manager)
    for service_name, service in services.items():
        get_tasks(service)
    #get initial stats
    #get web-worker stats
    
    get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, sql_cpu_avg, sql_mem_avg, web_worker_cpu_avg, web_worker_mem_avg)
    
    #initalize estimator
    
    init_x = np.asarray((sql_cpu_avg, web_worker_cpu_avg, sql_mem_avg, web_worker_mem_avg))
    init_x = init_x.reshape(init_x.size, 1)
    estimator = kalmanEstimator(np.identity(4), np.random.random((4,3)), init_x)
    #store measurements
    store_stats(sql_cpu_avg, sql_mem_avg, web_worker_mem_avg, web_worker_cpu_avg, num_web_workers, num_sql, num_requests, prev_sql_cpu_avg, prev_sql_mem_avg, prev_web_worker_mem_avg, prev_web_worker_cpu_avg, prev_num_web_workers, prev_num_sql, prev_num_requests)
    #Start generating a load
    process_list[0].start()
    #Wait a couple seconds
    time.sleep(5)
    #Send poll request to the process we started
    write_pipes[0].send["poll"]
    while not read_pipes[0].poll():
        pass
    #If the loop above has been broken then we can read the information from the pipe
    num_requests = read_pipes[0].recv()
    #get the stats
    get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, sql_cpu_avg, sql_mem_avg, web_worker_cpu_avg, web_worker_mem_avg)
    #create some np arrays for the regression
    sql_cpu_history = np.asarray(sql_cpu_avg - prev_sql_cpu_avg)
    sql_mem_history = np.asarray(sql_mem_avg - prev_sql_mem_avg)
    web_worker_cpu_history = np.asarray(web_worker_cpu_avg - prev_web_worker_cpu_avg)
    web_worker_mem_history = np.asarray(web_worker_mem_avg - prev_web_worker_mem_avg)
    request_history = np.asarray(num_requests - prev_num_requests)
    web_work_history = np.asarray(num_web_workers - prev_num_web_workers)
    sql_history = np.asarray(num_sql - prev_num_sql)
    #As before we store the stats
    store_stats(sql_cpu_avg, sql_mem_avg, web_worker_mem_avg, web_worker_cpu_avg, num_web_workers, num_sql, num_requests, prev_sql_cpu_avg, prev_sql_mem_avg, prev_web_worker_mem_avg, prev_web_worker_cpu_avg, prev_num_web_workers, prev_num_sql, prev_num_requests)
    #Wait a couple more seconds
    time.sleep(5)
    #Send poll request to the process we started
    write_pipes[0].send["poll"]
    while not read_pipes[0].poll():
        pass
    #If the loop above has been broken then we can read the information from the pipe
    num_requests = read_pipes[0].recv()
    #get the stats
    get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, sql_cpu_avg, sql_mem_avg, web_worker_cpu_avg, web_worker_mem_avg)
    #Append new values to the histories
    sql_cpu_history = np.append(sql_cpu_history, sql_cpu_avg - prev_sql_cpu_avg)
    sql_mem_history = np.append(sql_mem_history, sql_mem_avg - prev_sql_mem_avg)
    web_worker_cpu_history = np.append(web_worker_cpu_history, web_worker_cpu_avg - prev_web_worker_cpu_avg)
    web_worker_mem_history = np.append(web_worker_mem_history, web_worker_mem_avg - prev_web_worker_mem_avg)
    request_history = np.append(request_history, num_requests - prev_num_requests)
    web_work_history = np.append(web_work_history, num_web_workers - prev_num_web_workers)
    sql_history = np.append(sql_history, num_sql - prev_num_sql)
    #Store the stats
    store_stats(sql_cpu_avg, sql_mem_avg, web_worker_mem_avg, web_worker_cpu_avg, num_web_workers, num_sql, num_requests, prev_sql_cpu_avg, prev_sql_mem_avg, prev_web_worker_mem_avg, prev_web_worker_cpu_avg, prev_num_web_workers, prev_num_sql, prev_num_requests)
    
    #We now have 2 measurements in which the number of containers remained the same
    #We need at least 4 measurements to ensure that a solution can be found
    #Start 2 new containers
    num_web_workers = num_web_workers + 1
    num_sql = num_sql + 1
    scale(services["web-worker"], num_web_workers, manager)
    scale(services["mysql"], num_sql, manager)
    #We also start another load generator
    process_list[1].start()
    #as before we sleep and will update
    time.sleep(5)
    for i in range(0,2):
        write_pipes[i].send["poll"]
    pipes_ready = poll_pipes(read_pipes, 2)
    #reset number of requests
    num_requests = 0
    for i in range(0,2):
        num_requests = num_requests + read_pipes[i].recv()
    
    #update tasks since we scaled
    for service_name, service in services.items():
        get_tasks(service)
    #get the stats
    get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, sql_cpu_avg, sql_mem_avg, web_worker_cpu_avg, web_worker_mem_avg)
    #Append new values to the histories
    sql_cpu_history = np.append(sql_cpu_history, sql_cpu_avg - prev_sql_cpu_avg)
    sql_mem_history = np.append(sql_mem_history, sql_mem_avg - prev_sql_mem_avg)
    web_worker_cpu_history = np.append(web_worker_cpu_history, web_worker_cpu_avg - prev_web_worker_cpu_avg)
    web_worker_mem_history = np.append(web_worker_mem_history, web_worker_mem_avg - prev_web_worker_mem_avg)
    request_history = np.append(request_history, num_requests - prev_num_requests)
    web_work_history = np.append(web_work_history, num_web_workers - prev_num_web_workers)
    sql_history = np.append(sql_history, num_sql - prev_num_sql)
    #Store the stats
    store_stats(sql_cpu_avg, sql_mem_avg, web_worker_mem_avg, web_worker_cpu_avg, num_web_workers, num_sql, num_requests, prev_sql_cpu_avg, prev_sql_mem_avg, prev_web_worker_mem_avg, prev_web_worker_cpu_avg, prev_num_web_workers, prev_num_sql, prev_num_requests)
    #Now we get the 4th measurement
    #Scale down the number of sql containers and scale up web-worker
    num_sql = num_sql - 1
    num_web_workers = num_web_workers + 1
    scale(services["web-worker"], num_web_workers, manager)
    scale(services["mysql"], num_sql, manager)
    #as before we sleep and will update
    time.sleep(5)
    for i in range(0,2):
        write_pipes[i].send["poll"]
    pipes_ready = poll_pipes(read_pipes, 2)
    #reset number of requests
    num_requests = 0
    for i in range(0,2):
        num_requests = num_requests + read_pipes[i].recv()
        
    #get the stats
    get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, sql_cpu_avg, sql_mem_avg, web_worker_cpu_avg, web_worker_mem_avg)
    #Append new values to the histories
    sql_cpu_history = np.append(sql_cpu_history, sql_cpu_avg - prev_sql_cpu_avg)
    sql_mem_history = np.append(sql_mem_history, sql_mem_avg - prev_sql_mem_avg)
    web_worker_cpu_history = np.append(web_worker_cpu_history, web_worker_cpu_avg - prev_web_worker_cpu_avg)
    web_worker_mem_history = np.append(web_worker_mem_history, web_worker_mem_avg - prev_web_worker_mem_avg)
    request_history = np.append(request_history, num_requests - prev_num_requests)
    web_work_history = np.append(web_work_history, num_web_workers - prev_num_web_workers)
    sql_history = np.append(sql_history, num_sql - prev_num_sql)
    #Store the stats
    store_stats(sql_cpu_avg, sql_mem_avg, web_worker_mem_avg, web_worker_cpu_avg, num_web_workers, num_sql, num_requests, prev_sql_cpu_avg, prev_sql_mem_avg, prev_web_worker_mem_avg, prev_web_worker_cpu_avg, prev_num_web_workers, prev_num_sql, prev_num_requests)
    
    #Use these lines whenever we update the regression
    #TODO put this into a function
    target_mat = np.vstack([sql_cpu_history, web_worker_cpu_history, sql_mem_history, web_worker_mem_history]).T
    design_mat = np.vstack([sql_history, web_work_history, request_history]).T
    control_matrix = regregularized_lin_regression(design_mat, target_mat, 0.0001)
    estimator.update_B(control_matrix.T)
    
<<<<<<< HEAD
    
    
    close_flag = False
    while not close_flag:
        if input_pipe.poll():
            message = input_pipe.recv()
            if message == "close":
                close_flag = True
        
        #do the experiment here
    
=======
    #TODO We have generated an initial estimate
    #Begin by starting up the rest of the load generators and then monitoring and adjust
    
    
>>>>>>> 6f23a1a9f6633d63aa6e0a992556ac4a3de3aae7
