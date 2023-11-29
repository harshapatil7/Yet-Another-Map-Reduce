import http.server
import urllib
import json
import os
import requests
import threading

list_of_workers = []

class handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global list_of_workers

        query = urllib.parse.urlparse(self.path).query
        q = dict(q.split("=") for q in query.split("&"))
        NUM_OF_WORKERS, WORKER_BASE_PORT_NUMBER  = 1, 8000

        if(q['task'] == 'write'):
            self.send_response(200)
            self.send_header('Content-type','text/html')
            self.end_headers()

            with open('config.txt') as f:
                NUM_OF_WORKERS = int(f.readline().strip().split("=")[1])
                WORKER_BASE_PORT_NUMBER = int(f.readline().strip().split("=")[1])

            list_of_workers = []
            for i in range(WORKER_BASE_PORT_NUMBER, WORKER_BASE_PORT_NUMBER+NUM_OF_WORKERS):
                list_of_workers.append(i)

            self.wfile.write(bytes(str(list_of_workers), "utf8"))

        elif(q['task'] == 'worker_node'):
            self.send_response(200)
            self.send_header('Content-type','text/html')
            self.end_headers()
            print(f"WORKER AT PORT {q['PORT_NUM']} PINGED")
            # self.wfile.write(bytes("hellow", "utf8"))


        elif(q['task'] == 'read'):
            filename = q['file_to_read']
            extension = q['extension']


            #send manifest to client
            if(not os.path.exists(f"./metadata/{filename}_manifest") or not os.path.exists(f"./metadata/main_manifest")):
                print('not existing')
                self.send_response(500, message="File Not Found")
                self.send_header('Content-type','text/html')
                self.end_headers()
            else:
                with open(f"./metadata/{filename}_manifest", 'r') as f:
                    self.send_response(200)
                    self.send_header('Content-type','text/html')
                    self.end_headers()
                #make a request to send the file to all respective workers, to send file chunk to client
                with open("./metadata/main_manifest", "rb") as f:
                    main_manifest_data = None
                    if(os.path.getsize("./metadata/main_manifest") > 0):
                        main_manifest_data = json.load(f)
                        list_of_ports = main_manifest_data[f"{filename}.{extension}"]
                        req_param = {'task' : 'send_to_client', 'filename' : filename, 'extension' : extension}
                        
                        # print('sending get req to each worker')
                        #send req to worker for sending file chunks to client
                        for each_port in list_of_ports:
                            try:
                                requests.get(f"http://localhost:{each_port}", params=req_param, timeout=0.00001)
                                # print('made req to :', each_port)
                            except Exception:
                                pass
                    # print('ended get req loop in master')
                    
                with open(f"./metadata/{filename}_manifest", 'r') as f:
                    obj_to_send = {
                        'NUM_OF_WORKERS' : list_of_ports,
                        'FILE' : f.read()
                    }
                    self.wfile.write(json.dumps(obj_to_send).encode())

                


    def do_POST(self):
        global list_of_workers

        query = urllib.parse.urlparse(self.path).query
        q = dict(q.split("=") for q in query.split("&"))

        #GET NUM OF WORKERS, PORTS
        NUM_OF_WORKERS, WORKER_BASE_PORT_NUMBER  = 1, 8000
        with open('config.txt') as f:
            NUM_OF_WORKERS = int(f.readline().strip().split("=")[1])
            WORKER_BASE_PORT_NUMBER = int(f.readline().strip().split("=")[1])

        if(q['task'] == 'manifest'):
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)

            file_name = q['filename']

            with open(f"./metadata/{file_name}", "wb") as f:
                f.write(post_data)


            #write port list to file key val pair in main manifests file
            main_manifest_data = {}
            fname = file_name.split("_")[0] + '.' + q['extension']

            if(os.path.exists("./metadata/main_manifest")):
                with open("./metadata/main_manifest", "r") as f:
                    if(os.path.getsize("./metadata/main_manifest") > 0):
                        main_manifest_data = json.load(f)

            main_manifest_data[fname] = list_of_workers

            with open("./metadata/main_manifest", "w") as f:
                json.dump(main_manifest_data, f)

            print("MANIFEST WRITTEN SUCCESSFULLY BY MASTER")

            self.send_response(200)
            self.send_header('Content-type','text/html')
            self.end_headers()
            
            message = "Write Operation Completed Successfully"
            self.wfile.write(bytes(message, "utf8"))

        elif(q['task'] == 'mapper_reducer'):
            mapper_reducer = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            file_name = q['file_name']

            #Initialize Map operation
            #Send filename, mapper to all workers CONTAINING THE FILE and initiate the operation
            list_of_workers = []
            try:
                with open(os.path.join('metadata', 'main_manifest'), 'r') as f:
                    main_manifest = json.load(f)
                    list_of_workers = main_manifest[file_name]
            except Exception:
                self.send_response(500, "File Not Found in Metadata")
                self.send_header('Content-type','text/html')
                self.end_headers()

            params = {'task' : 'mapper', 'filename' : file_name}
            
            t = []
            responses = []
            for i in list_of_workers:
                # print('SENDING MAPPER TO ', i)
                new_thread = threading.Thread(target=send_mapper_to_worker, args=[i, params, mapper_reducer, responses])
                t.append(new_thread)
                new_thread.start()

            for thr in t:
                thr.join()

            all_workers_success = True
            #check if all mappers returned status 200
            for res in responses:
                if res.status_code != 200:
                    all_workers_success = False


            if(not all_workers_success):
                self.send_response(500, "ERROR IN WORKER MAP EXECUTION")
                return
            
            print('==============================================')
            print('MAP TASK FINISHED')
            print('==============================================')


            # ######################## LOG MAP TASK COMPLETED
            t = []
            responses = []
            params = {'task': 'shuffle', 'filename': file_name}
            main_manifest = {}
            with open( os.path.join('metadata','main_manifest'),"r") as f:
                main_manifest = json.load(f)

            for i in list_of_workers:
                new_thread = threading.Thread(target=send_shuffle_request, args=[file_name, i, params,responses, main_manifest])
                t.append(new_thread)
                new_thread.start()

            for thr in t:
                thr.join()

            all_workers_success = True
            #check if all mappers returned status 200
            for res in responses:
                if res.status_code != 200:
                    all_workers_success = False
            if(not all_workers_success):
                self.send_response(500, "ERROR IN WORKER SHUFFLE EXECUTION")
                return


            #SEQUENTIALLY
            # for i in list_of_workers:
            #     send_shuffle_request(file_name, i, params,responses, main_manifest)
            
            print('==============================================')
            print('SHUFFLE FINISHED')
            print('==============================================')


            #LOG SHUFFLE COMPLETED

            #send reducer to each node
            params = {'task' : 'reducer', 'filename' : file_name}
            t = []
            responses = []
            for i in list_of_workers:
                # print('SENDING REDUCER TO ', i)
                new_thread = threading.Thread(target=send_reducer_to_worker, args=[i, params, mapper_reducer, responses])
                t.append(new_thread)
                new_thread.start()

            for thr in t:
                thr.join()

            all_workers_success = True
            #check if all mappers returned status 200
            for res in responses:
                if res.status_code != 200:
                    all_workers_success = False


            if(not all_workers_success):
                self.send_response(500, "ERROR IN WORKER MAP EXECUTION")
                return
            
            print('==============================================')
            print('REDUCE FINISHED')
            print('==============================================')


            #create manifest file for output
            with open(os.path.join('metadata', f"{file_name.split('.')[0]}_part-00000_manifest"), "w") as f:
                f.write("filename,filesize,header\n")
                for res in responses:
                    f.write(f"{res.text.split('---')[0]},{res.text.split('---')[1]},False\n")

            main_manifest_data = {}
            with open("./metadata/main_manifest", "r") as f:
                if(os.path.getsize("./metadata/main_manifest") > 0):
                    main_manifest_data = json.load(f)

            main_manifest_data[f"{file_name.split('.')[0]}_part-00000.{file_name.split('.')[1]}"] = list_of_workers

            with open("./metadata/main_manifest", "w") as f:
                json.dump(main_manifest_data, f)

            self.send_response(200)
            self.send_header('Content-type','text/html')
            self.end_headers()
                



def send_mapper_to_worker(i, params, mapper_reducer, responses):
    responses.append(requests.post(f"http://localhost:{i}", data=json.dumps(mapper_reducer['mapper_file']).encode(), params=params))

def send_reducer_to_worker(i, params, mapper_reducer, responses):
    responses.append(requests.post(f"http://localhost:{i}", data=json.dumps(mapper_reducer['reducer_file']).encode(), params=params))

def send_shuffle_request(file_name, i, params, responses, main_manifest):
    responses.append(requests.post(f"http://localhost:{i}", data=json.dumps(main_manifest[file_name]).encode() ,params=params))



