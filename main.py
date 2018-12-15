import multiprocessing
import time
from controller import controller


def main():
    #change this to control the number of load processes
    number_of_load_processes = 5
    input_pipe, output_pipe = multiprocessing.Pipe()
    controller_process = multiprocessing.Process(target=controller, args(input_pipe, output_pipe, number_of_load_processes))
    controller_process.start()
    s = input('Type Quit to Quit')
    input_pipe.send([s])
    
    
if __name__ == "__main__":
    main()