import multiprocessing
import time
import argparse
from controller import controller
from logger import logger
from utils import get_stats, getNodeIDs, get_tasks, getServices

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node_list", required=True, nargs='+', help='list of node IPs')
    parser.add_argument("--req_list", required=True, nargs='+')
    parser.add_argument("--manager", required=True, help='manager node IP')
    parser.add_argument("--number_loads", type=int, default=5, help='number of load processes to spawn')
    parser.add_argument("--poll_interval", type=float, default=5.0, help='number of seconds between polls')
    parser.add_argument("--polls_per_update", type=int, default=3, help='number of polls between updates to the estimator')
    parser.add_argument("--log_file", required=True, help="path to log file to create")
    args = parser.parse_args()

    #number_loads = 2
    #node_list = ["192.168.56.101:4000", "192.168.56.102:4000", "192.168.56.103:4000"]
    #manager = "192.168.56.101:4000"

    # change this to control the number of load processes
    # number_of_load_processes = 5
    services = {}
    nodes = {}
    input_pipe, output_pipe = multiprocessing.Pipe()
    getNodeIDs(args.node_list, nodes)
    getServices(services, args.manager)
    controller_process = multiprocessing.Process(target=controller, args=(output_pipe, args.number_loads, args.node_list, args.req_list, args.manager, args.poll_interval, args.polls_per_update, args.log_file, nodes, services))
    
    controller_process.start()
    time.sleep(120)
    #log_process.start()
    #time.sleep(40)
    s = input('Type Quit to Quit ')
    input_pipe.send(s)
    controller_process.join()
    #log_process.join()


if __name__ == "__main__":
    main()