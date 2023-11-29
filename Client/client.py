import sys
import os
import requests
from json import loads
import http.server
import socketserver
from filesplit.split import Split
from filesplit.merge import Merge
import math
import urllib
import threading
import json
import shutil

list_of_nodes = []
worker_replies = 0


class handler(http.server.BaseHTTPRequestHandler):

    def do_GET(self):
        pass

    def do_POST(self):
        global list_of_nodes, worker_replies

        query = urllib.parse.urlparse(self.path).query
        q = dict(q.split("=") for q in query.split("&"))
        
        if(q['task'] == 'file_chunk'):
            # print('file chunk post req received')
            # print(q['filename'])


            with open(f"./temp/{q['filename']}", 'wb') as f:
                f.write(self.rfile.read(int(self.headers['Content-Length'])))

            self.send_response(200, message="File Chunk Received")
            self.send_header('Content-type','text/html')
            self.end_headers()
            worker_replies+=1 



class Client:

    flags = ['-r', '-w', '-mr']
    PORT = 8000
    IP = "http://127.0.0.1"

    def __init__(self):
        # try:
        self.task = sys.argv[1]

        if self.task not in self.flags:
            raise Exception()

        if (not os.path.exists('temp')):
            os.mkdir('temp')

        # except Exception as e:
        # 
        #     print("Invalid Command, Please use this format\npython client.py -[r | w | mr] [filename.py]  ")
        #     exit(-1)

        if(self.task == self.flags[0]):
            self.read()
        elif(self.task == self.flags[1]):
            self.write()
        elif(self.task == self.flags[2]):
            self.mr()

        
    def read(self):
        global list_of_nodes, worker_replies
        full_filename = os.path.basename(sys.argv[2])


        with socketserver.TCPServer(("", self.PORT), handler) as httpd:
            print("serving read server at port", self.PORT)
            filename = full_filename.split('.')[0]
            extension = full_filename.split('.')[1]
            param = {'task' : 'read', 'file_to_read' : filename, 'extension' : extension}
            res = requests.get(self.IP + ":3000", params=param)
            print('manifest received from master')
            info = json.loads(res.content)
            list_of_nodes = info['NUM_OF_WORKERS']

            with open(f"./temp/{filename}_manifest", 'w') as f:
                f.write(info['FILE'])

            while(worker_replies != len(list_of_nodes)):
                httpd.handle_request()
            
            worker_replies = 0

            #merge the file and print to user
            file_to_merge = os.path.basename(sys.argv[2])
            merge = Merge('temp', 'temp', file_to_merge)
            merge.manfilename = f"{filename}_manifest"
            merge.merge()

            #read file out to stdout
            data = ""
            with open(f"temp/{file_to_merge}", 'r') as f:
                data = f.readlines()
                data.sort()
                data = "".join(data)
            with open(os.path.join('temp', os.path.basename(sys.argv[2])), 'w') as to_write:
                to_write.write(data)
            print(data)
            


    def write(self):
        global list_of_nodes

        with socketserver.TCPServer(("", self.PORT), handler) as httpd:
            print("serving write server at port", self.PORT)
            param = {'task' : 'write'}
            req = requests.get(self.IP + ":3000", params=param)
            list_of_nodes = loads(req.content.decode())
            self.split_and_send(os.path.join(os.getcwd(), sys.argv[2]))

        shutil.rmtree('temp')

    def mr(self):
        file_name = os.path.join(os.getcwd(), sys.argv[2])
        mapper_file = os.path.join(os.getcwd(), sys.argv[3])
        reducer_file = os.path.join(os.getcwd(), sys.argv[4])

        with socketserver.TCPServer(("", self.PORT), handler) as httpd:
            print("serving map reduce server at port", self.PORT)
            self.sendmapperreducer(file_name, mapper_file, reducer_file)

        
        print("MAP REDUCE TASK COMPLETE")
        print('--------------')
        print("FILE SAVED AS : ")
        fname = os.path.basename(file_name)
        print(fname.split('.')[0] + "_part-00000." + fname.split('.')[1])
        print('--------------')

        shutil.rmtree('temp')


    def split_and_send(self, file_loc):
        global list_of_nodes

        if(not os.path.exists(file_loc)):
            raise Exception("File Invalid")
        
        filename = os.path.basename(file_loc).split('.')[0]
        extension = os.path.basename(file_loc).split('.')[1]

        PATH_TO_TEMP = os.path.join(os.getcwd() ,'temp')
        if(not os.path.exists(PATH_TO_TEMP)):
            os.mkdir(PATH_TO_TEMP)

        num_of_nodes = len(list_of_nodes)
        split_lines = 0
        with open(file_loc, 'r') as f:
            lines_in_file = len(f.readlines())
            if(num_of_nodes > lines_in_file):
                split_lines = 1
                #create remaining blank files
                for i in range(lines_in_file+1, lines_in_file+(num_of_nodes-lines_in_file)+1):
                    with open(os.path.join(PATH_TO_TEMP, f"{filename}_{i}.{extension}"), 'w') as f:
                        pass
            else:
                split_lines = math.ceil(lines_in_file/num_of_nodes)
                #create remaining blank files
                for i in range(int(lines_in_file/split_lines)+1, num_of_nodes+1):
                    with open(os.path.join(PATH_TO_TEMP, f"{filename}_{i}.{extension}"), 'w') as f:
                        pass

        split = Split(file_loc, PATH_TO_TEMP)
        split.bylinecount(split_lines)

        #SEND OPERATION
        for i in range(num_of_nodes):
            to_read_from = os.path.join(PATH_TO_TEMP, f"{filename}")
            with open(f"{to_read_from}_{i+1}.{extension}", 'rb') as f:
                params = {'task' : 'write_file', 'filename':f"{filename}_{i+1}.{extension}"}
                res = requests.post(f"{self.IP}:{list_of_nodes[i]}", data=f.read(), params=params)
        
        #SEND MANIFEST TO MASTER
        params = {'task': 'manifest', 'filename': f"{filename}_manifest" ,'extension':extension}
        with open(f"{PATH_TO_TEMP}/manifest", 'rb') as f:
            res = requests.post(f"{self.IP}:3000", data=f.read(), params=params)
            # print(res.content.decode())

    
    def sendmapperreducer(self, file_name, mapper_file, reducer_file):
        filename = os.path.basename(file_name)
        mapper_reducer = {} 
        with open(mapper_file, "r") as f:
            mapper_reducer['mapper_file'] = f.read()
        with open(reducer_file, "r") as f:
            mapper_reducer['reducer_file'] = f.read()

        params = {'task': 'mapper_reducer', 'file_name' : filename}
        res = requests.post(f"{self.IP}:3000", data=json.dumps(mapper_reducer).encode(), params=params)

clientObj = Client()