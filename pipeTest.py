import multiprocessing, csv

def f(pipe):
    while not pipe.poll():
        pass
    x = pipe.recv()
    y = x * x
    pipe.send(y)
    
def g(pipe):
    while not pipe.poll():
        pass
    print(pipe.recv())
    pipe.close()
    
    
def h(pipe):
    while not pipe.poll():
        pass
    tup = pipe.recv()
    print(tup[0])
    pipe.close()
    

def d(pipe):
    while not pipe.poll():
        pass
    tup = pipe.recv()
    f = open(tup[0], 'w', newline='')
    testWriter = csv.writer(f, dialect='excel')
    testWriter.writerow(tup[1:])
    f.close()
    
    
def main():
    pipe_1, pipe_2 = multiprocessing.Pipe()
    #p1 = multiprocessing.Process(target=f, args=(pipe_1,))
    p2 = multiprocessing.Process(target=h, args=(pipe_2,))
    #p1.start()
    p2.start()
    pipe_1.send('x')
    #pipe_1.send(['test2.csv', 5, 'Two', 4, 5, 6, 7, 'nine'])
   #p1.join()
    p2.join()
    
    
if __name__ == '__main__':
    main()