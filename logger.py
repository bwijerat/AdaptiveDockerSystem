import csv, multiprocessing, time
from utils import get_stats, getNodeIDs, get_tasks, getServices
def logger(pipe, log_file, node_list, manager, startTime, interval, nodes, services, close_pipe):
    f = open("{0}_results.csv".format(log_file), 'w', newline='')
    f2 = open("{0}_observations.csv".format(log_file), 'w', newline='')
    close_flag = False
    logWriter = csv.writer(f, dialect='excel')
    logWriter2 = csv.writer(f2, dialect='excel')
    #services = {}
    #nodes = {}
    #getNodeIDs(node_list, nodes)
    #getServices(services, manager)
    sql_cpu_avg = 0
    web_worker_cpu_avg = 0
    sql_mem_avg = 0
    web_worker_mem_avg = 0
    sql_cpu_usages = []
    sql_mem_usages = []
    web_worker_cpu_usages = []
    web_worker_mem_usages = []
    for service_name, service in services.items():
        get_tasks(service, manager)
    sql_cpu_avg, web_worker_cpu_avg, sql_mem_avg, web_worker_mem_avg = get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, nodes)
    diff_time = time.time() - startTime
    logWriter2.writerow([sql_cpu_avg, web_worker_cpu_avg, sql_mem_avg, web_worker_mem_avg, diff_time//60, diff_time%60])
    while not close_flag:
        while not close_pipe.poll():
            time.sleep(interval)
            sql_cpu_usages = []
            sql_mem_usages = []
            web_worker_cpu_usages = []
            web_worker_mem_usages = []
            if pipe.poll():
                pipe_tuple = pipe.recv()
                if pipe_tuple == "close":
                    print("Logger shutting down")
                    close_flag = True
                    f.close()
                    f2.close()
                else:
                    if pipe_tuple[10] == True:
                    #time.sleep(interval)
                        for service_name, service in services.items():
                            get_tasks(service, manager)
                
                    logWriter.writerow(pipe_tuple[:10])
            sql_cpu_avg, web_worker_cpu_avg, sql_mem_avg, web_worker_mem_avg = get_stats(services, sql_cpu_usages, sql_mem_usages, web_worker_cpu_usages, web_worker_mem_usages, nodes)
            diff_time = time.time() - startTime
            logWriter2.writerow([sql_cpu_avg, web_worker_cpu_avg, sql_mem_avg, web_worker_mem_avg, diff_time//60, diff_time%60])
            
        while not close_flag:
            if pipe.poll():
                pipe_tuple = pipe.recv()
                if pipe_tuple == "close":
                    print("Logger shutting down")
                    close_flag = True
                    f.close()
                    f2.close()
                else:                
                    logWriter.writerow(pipe_tuple[:10])