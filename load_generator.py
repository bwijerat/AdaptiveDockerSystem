import json
import requests
import time
from urllib.request import urlopen
from multiprocessing import Pipe
import random
import string

def randomString(n):
    #Generates a random string of length n
    return ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=n))
    

#a function to generate a load
def load_process(node_list, write_connection, read_connection):
    #flag to indicate whether or not to close
    close_flag = False
    requests_sent = 0
    number_of_nodes = len(node_list)
    while not close_flag:
        if read_connection.poll():
            message = read_connection.recv()
            if message == "close":
                close_flag = True
            elif message == "poll":
                write_connection.send(requests_sent)
                requests_sent = 0
        #pick a random node to send the request to
        node_number = random.randint(0, number_of_nodes - 1)
        #pick a operation
        #There are 3 possible operations
        operation_number = random.randint(0, 2)
        if operation_number == 0:
            #Calculate pi
            r = requests.get('{0}/dataop/pi'.format(node_list[node_number]))
        elif operation_number == 1:
            #Select a number of users
            number_to_select = random.randint(1, 100)
            r = requests.get('{0}/dataop/SelectUsers?recordsCnt={1}'.format(node_list[node_number], number_to_select))
        elif operation_number == 2:
            #Insert a new user
            r = requests.get('{0}/dataop/InsertUsers?fName={1}&lName={2}'.format(node_list[node_number], randomString(random.randint(1, 10)), randomString(random.randint(1, 10))))
        requests_sent = requests_sent + 1
        #To prevent sending too many requests the process will sleep for a random number of miliseconds between 10 and 50
        sleep_mili_interval = random.uniform(10.0, 50.0)
        #time.sleep expects values in seconds. Divide by 1000 to maintain miliseconds
        time.sleep(sleep_mili_interval/1000.0)