import socketserver
import requests
import sys
import json
import http.server
import urllib
import os
from subprocess import Popen, PIPE, STDOUT
import hashlib
import signal
import threading
from concurrent.futures import ThreadPoolExecutor

PORT = 9000
BG_SERVER = False

class handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global PORT
        query = urllib.parse.urlparse(self.path).query
        q = dict(q.split("=") for q in query.split("&"))

        if(q['task'] == 'send_to_client'):
            #send file chunk to client
            list_of_files = os.listdir(f"WORKER_{PORT}")

            filename = q['filename']
            extension = q['extension']


            if('_part-00000' in filename):
                filename = filename.split('_part-00000')[0] + "_" + str(PORT)

            file_chunk_to_send = [fname for fname in list_of_files if (fname.startswith(filename) and fname.endswith(extension))][0]
            # print(f"sending {file_chunk_to_send}")
            with open(os.path.join(f"WORKER_{PORT}", file_chunk_to_send), 'rb') as f:
                res = requests.post("http://localhost:8000", data=f.read() , params={'task' : 'file_chunk', 'filename':file_chunk_to_send})
                print('done sending worker :', PORT)

            self.send_response(200, message="Sent File")
            self.send_header('Content-type','text/html')
            self.end_headers()



    def do_POST(self):
        global PORT
        query = urllib.parse.urlparse(self.path).query
        q = dict(q.split("=") for q in query.split("&"))


        if(q['task'] == 'write_file'):
            content_len = int(self.headers['Content-Length'])
            file_name = q['filename']

            if(content_len == 0):
                #create blank file because we did not receive any data
                with open(os.path.join(f"WORKER_{PORT}", file_name), "wb") as f:
                    pass
            else:
                post_data = self.rfile.read(content_len)

                with open(os.path.join(f"WORKER_{PORT}", file_name), "wb") as f:
                    f.write(post_data)
                    
            # print("WRITTEN SUCCESSFULLY BY WORKER : ", PORT)
            self.send_response(200)
            self.send_header('Content-type','text/html')
            self.end_headers()


        elif(q['task'] == 'mapper'):
            filename = q['filename'].split('.')[0]
            extension = q['filename'].split('.')[1]

            mapper = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            with open(os.path.join(f"WORKER_{PORT}", "mapper.py"), "w") as f:
                f.write(mapper)
            # print("MAPPER RECEIVED BY WORKER : ", PORT)

            #EXECUTE MAPPER HERE
            list_of_files = os.listdir(f"WORKER_{PORT}")
            file_chunk_to_read = [fname for fname in list_of_files if (fname.startswith(filename) and fname.endswith(extension))][0]

            with open( os.path.join(f"WORKER_{PORT}", file_chunk_to_read), "rb") as to_read:
                path_to_mapper = os.path.join(os.getcwd(), f"WORKER_{PORT}", "mapper.py")

                mapper_subp = Popen(['python', path_to_mapper], stdout=PIPE, stdin=PIPE, stderr=PIPE)

                #write mapper output to intermediate output dir
                intermediate_output_path = os.path.join(f"WORKER_{PORT}", 'MAPPER_OUTPUT')

                if(not os.path.exists(intermediate_output_path)):
                    os.mkdir(intermediate_output_path)
                with open(os.path.join(intermediate_output_path, f"{filename}.{extension}"), 'wb') as f:
                    f.write(mapper_subp.communicate(input=to_read.read())[0])
                # print("GOT OUTPUT!")

            self.send_response(200)
            self.send_header('Content-type','text/html')
            self.end_headers()


        elif(q['task'] == 'shuffle_file'):
            full_filename = q['filename']

            with open(os.path.join(f"WORKER_{PORT}", 'MAPPER_OUTPUT', f"SHUFFLED_{full_filename}"), 'ab') as f:
                f.write(self.rfile.read(int(self.headers['Content-Length'])))


        elif(q['task'] == 'shuffle'):
            filename = q['filename'].split('.')[0]
            extension = q['filename'].split('.')[1]
            full_filename = q['filename']
            list_of_ports = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            num_of_ports = len(list_of_ports)
            
            d = {}

            with open(os.path.join(f"WORKER_{PORT}", "MAPPER_OUTPUT", full_filename), 'r') as mapper_output:
                for line in mapper_output.readlines():
                    key = line.split(',')[0]
                    hashed_port = list_of_ports[hash_value(key, num_of_ports)]
                    if hashed_port in d:
                        d[hashed_port] += line
                    else:
                        d[hashed_port] = line


            with open(os.path.join(f"WORKER_{PORT}", 'MAPPER_OUTPUT', f"SHUFFLED_{full_filename}"),'w') as f:
                f.write(d[PORT])

            def send_shuffle(to_port):
                if to_port == PORT:
                    return
                try:
                    requests.post(f"http://localhost:{to_port}", data=d[to_port].encode(), params={'task' : 'shuffle_file', 'filename' : full_filename})
                except Exception:
                    pass


            # t = []
            # for p in list_of_ports:
            #     temp = threading.Thread(target=send_shuffle, args=[p], daemon=True)
            #     t.append(temp)
            #     temp.start()

            # for th in t:
            #     th.join()


            # print(to_send_to)
            # with ThreadPoolExecutor(max_workers=10) as executor:
            #     resp_list = list(executor.map(send_shuffle, to_send_to))


            for p in list_of_ports:
                try:
                    requests.post(f"http://localhost:{p}", data=d[p].encode(), params={'task' : 'shuffle_file', 'filename' : full_filename}, timeout=0.000001)
                except Exception:
                    pass





            self.send_response(200, message="Shuffling Complete")
            self.send_header('Content-type','text/html')
            self.end_headers()



        elif(q['task'] == 'reducer'):
            filename = q['filename'].split('.')[0]
            extension = q['filename'].split('.')[1]
            full_filename = q['filename']

            #sort the data for inputting
            data = ""
            with open(os.path.join(f"WORKER_{PORT}", 'MAPPER_OUTPUT', f"SHUFFLED_{full_filename}"), 'r') as f:
                data = f.readlines()
            data.sort()
            data = ''.join(data).encode()
            
            
            reducer = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            with open(os.path.join(f"WORKER_{PORT}", "reducer.py"), "w") as f:
                f.write(reducer)
            # print("REDUCER RECEIVED BY WORKER : ", PORT)

            #EXECUTE REDUCER HERE
            path_to_reducer = os.path.join(os.getcwd(), f"WORKER_{PORT}", "reducer.py")
            mapper_subp = Popen(['python', path_to_reducer], stdout=PIPE, stdin=PIPE, stderr=PIPE)
            #write mapper output to intermediate output dir
            intermediate_output_path = os.path.join(f"WORKER_{PORT}")
            if(not os.path.exists(intermediate_output_path)):
                os.mkdir(intermediate_output_path)
            with open(os.path.join(intermediate_output_path, f"{filename}_{PORT}.{extension}"), 'wb') as f:
                f.write(mapper_subp.communicate(input=data)[0])

            size = os.path.getsize(os.path.join(intermediate_output_path, f"{filename}_{PORT}.{extension}"))
            s = f"{filename}_{PORT}.{extension}---{size}" 

            # print("SENDING : ", s)
            self.send_response(200, message=s)
            self.send_header('Content-type','text/html')
            self.end_headers()
            self.wfile.write(s.encode())




def hash_value(value, num_of_ports):
    return (int(hashlib.md5(str(value).encode()).hexdigest(), 16) % num_of_ports)


def create_requried_directories(to_send_path, shuffle_output_path, received_path):
    global PORT

    if(not os.path.exists(to_send_path)):
        os.mkdir(to_send_path)
    if(not os.path.exists(shuffle_output_path)):
        os.mkdir(shuffle_output_path)
    if(not os.path.exists(received_path)):
        os.mkdir(received_path)




class Worker:
    NUM_OF_WORKERS=0
    IP = "http://127.0.0.1"

    def __init__(self):
        global PORT, BG_SERVER
        BG_SERVER = True
        PORT = int(sys.argv[1])
        #Change Directory to current directory
        os.chdir(os.path.join(os.path.pardir, 'Worker'))
        self.create_write_folder()


        with socketserver.TCPServer(("", PORT), handler) as httpd:
            print("serving worker server at port", PORT)
            param = {'task' : 'worker_node', 'PORT_NUM' : PORT}
            req = requests.get(self.IP + ":3000", params=param)
            httpd.serve_forever()


    def create_write_folder(self):
        global PORT
        if(not os.path.exists(f"WORKER_{PORT}")):
            os.mkdir(f"WORKER_{PORT}")

worker_obj = Worker()