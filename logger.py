import csv, multiprocessing

def logger(pipe, log_file):
    f = open(log_file, 'w', newline='')
    close_flag = False
    logWriter = csv.writer(f, dialect='excel')
    while not close_flag:
        while not pipe.poll():
            pass
        pipe_tuple = pipe.recv()
        if pipe_tuple[0] == "close":
            close_flag = True
            f.close()
        else:
            logWriter.writerow(pipe_tuple)