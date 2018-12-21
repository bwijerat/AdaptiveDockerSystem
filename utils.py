import json
import requests
import time
import math
from urllib.request import urlopen

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
        #print("{service} tasks:".format(service=service["name"]))
        for task in data:
            if task["Status"]["State"] == "running":
                container_id = task["Status"]["ContainerStatus"]["ContainerID"]
            else:
                continue
            node_id = task["NodeID"]
            service["tasks"].append({"ContainerID": container_id, "NodeID": node_id})
            #print('''\t ContainerID: {}, NodeID: {} '''.format(container_id, node_id))


# get all NodeIDs in swarm
# nodes = {}
# print("Nodes:")
# Get node IDs
def getNodeIDs(node_list, nodes):
    for node in node_list:
        with urlopen("http://{node}/info".format(node=node)) as url:
            data = json.loads(url.read().decode())
            nodes[data["Swarm"]["NodeID"]] = node
            #print('''\t NodeID: {} '''.format(
                #data["Swarm"]["NodeID"], ))


# list all the services
# services = {}
def getServices(services, manager):
    with urlopen("http://{manager}/services".format(manager=manager)) as url:
        data = json.loads(url.read().decode())
        #print("Services:")
        for service in data:
            services[service["Spec"]["Name"]] = {"name": service["Spec"]["Name"], "tasks": []}
            #print('''\t name: {}, version: {}, replicas: {}  '''.format(
                #service["Spec"]["Name"],
                #service["Version"]["Index"],
                #service["Spec"]["Mode"]["Replicated"]["Replicas"]))


# get the tasks running on our swarm cluster

# for service_name, service in services.items():
# get_tasks(service)

def get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, nodes):
    for task in services["web-worker"]["tasks"]:
        try:
            with urlopen('http://{node}/containers/{containerID}/stats?stream=false'.format(
                node=nodes[task["NodeID"]], containerID=task["ContainerID"])) as url:
                data = json.loads(url.read().decode())
                task_cpu, task_mem = calculate_cpu_and_mem_percent(data)
                web_worker_cpu_usages.append(task_cpu)
                web_worker_mem_usages.append(task_mem)
        except:
            print('http://{node}/containers/{containerID}/stats?stream=false'.format(
                node=nodes[task["NodeID"]], containerID=task["ContainerID"]))
    # repeat for sql stats
    for task in services["mysql"]["tasks"]:
        with urlopen('http://{node}/containers/{containerID}/stats?stream=false'.format(
                node=nodes[task["NodeID"]], containerID=task["ContainerID"])) as url:
            data = json.loads(url.read().decode())
            task_cpu, task_mem = calculate_cpu_and_mem_percent(data)
            sql_cpu_usages.append(task_cpu)
            sql_mem_usages.append(task_mem)
    # get averages
    sql_cpu_avg = float(sum(sql_cpu_usages) / len(sql_cpu_usages))
    sql_mem_avg = float(sum(sql_mem_usages) / len(sql_mem_usages))
    web_worker_cpu_avg = float(sum(web_worker_cpu_usages) / len(web_worker_cpu_usages))
    #print(web_worker_cpu_avg)
    web_worker_mem_avg = float(sum(web_worker_mem_usages) / len(web_worker_mem_usages))
    return sql_cpu_avg, web_worker_cpu_avg, sql_mem_avg, web_worker_mem_avg