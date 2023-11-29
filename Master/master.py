import socketserver
import os
from master_handler import handler
import subprocess

#Class Master
class Master:
    PORT = 3000
    NUM_OF_WORKERS=1
    WORKER_BASE_PORT_NUMBER=8000

    def __init__(self):

        with open('config.txt') as f:
            self.NUM_OF_WORKERS = int(f.readline().strip().split("=")[1])
            self.WORKER_BASE_PORT_NUMBER = int(f.readline().strip().split("=")[1])

        for i in range(self.WORKER_BASE_PORT_NUMBER, self.WORKER_BASE_PORT_NUMBER+self.NUM_OF_WORKERS):
            list_of_workers.append(subprocess.Popen(["python", "D:\Yamr\Worker\worker.py", f"{i}"], shell=True))

        metadata_path = os.path.join(os.getcwd(), 'metadata')
        if(not os.path.exists(metadata_path)):
            os.mkdir('metadata')

        with socketserver.TCPServer(("", self.PORT), handler) as httpd:
            print("serving master server at port", self.PORT)
            httpd.serve_forever()
            



list_of_workers = []




instance_master = Master()
