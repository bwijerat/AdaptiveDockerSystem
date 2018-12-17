import multiprocessing
import time
import argparse
from controller import controller


def main():
    #parser = argparse.ArgumentParser()
    #parser.add_argument("--node_list", required=True, nargs='+', help='list of node IPs')
    #parser.add_argument("--manager", required=True, help='manager node IP')
    #parser.add_argument("--number_loads", type=int, default=5, help='number of load processes to spawn')
    #args = parser.parse_args()

    number_loads = 2
    node_list = ["192.168.56.101:4000", "192.168.56.102:4000", "192.168.56.103:4000"]
    manager = "192.168.56.101:4000"

    # change this to control the number of load processes
    # number_of_load_processes = 5
    input_pipe, output_pipe = multiprocessing.Pipe()
    controller_process = multiprocessing.Process(target=controller, args=(input_pipe, output_pipe, number_loads, node_list, manager))
    controller_process.start()
    s = input('Type Quit to Quit')
    input_pipe.send([s])
    controller_process.join()


if __name__ == "__main__":
    main()