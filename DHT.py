import socket
import threading
import os
import time
import hashlib
import json


class Node:
    def __init__(self, host, port):
        self.stop = False
        self.host = host
        self.port = port
        self.M = 16
        self.N = 2**self.M
        self.key = self.hasher(host+str(port))
        
        # You will need to kill this thread when leaving
        threading.Thread(target = self.listener).start()
        self.files = []
        self.backUpFiles = []
        if not os.path.exists(host+"_"+str(port)):
            os.mkdir(host+"_"+str(port))


        # Set value of the following variables appropriately to pass Intialization test
        self.successor = (self.host, self.port)
        self.predecessor = (self.host, self.port)
        # additional state variables
        self.second_successor = (self.host, self.port)
        self.pinging = threading.Thread(target=self.periodic_pinging, args=())

    def hasher(self, key):
        '''
    	For a node: self.hasher(node.host+str(node.port))
    	For a file: self.hasher(file)
        '''
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N

    def handleConnection(self, client, addr):
        '''
        Function to handle each inbound connection, called as a thread from the listener.
        '''
        client.send(json.dumps(["connected"]).encode("utf-8"))
        while not self.stop:
            message = json.loads(client.recv(1024).decode("utf-8"))
            message_type = message[0]

            if message_type == "join":
                if self.successor == (self.host, self.port): # Second node joining
                    msg = json.dumps(["twoNodeSystem"])
                    client.send(msg.encode("utf-8"))
                    self.successor = (message[2][0], message[2][1])
                    self.predecessor = self.successor
                    # start pinging
                    self.pinging.start()
                else:
                    successor = self.lookup(message[1])
                    to_send = ["setSuccessor", successor]
                    client.send(json.dumps(to_send).encode("utf-8"))

                break

            elif message_type == "getKey":
                client.send(json.dumps(["myKey", self.key]).encode("utf-8"))
                break

            elif message_type == "lookUpRequest":
                node = self.lookup(message[1])
                to_send = ["found", node]
                client.send(json.dumps(to_send).encode("utf-8"))
                break

            elif message_type == "setPredecessor":
                to_send = ["setPredecessor", self.predecessor, self.successor]
                client.send(json.dumps(to_send).encode("utf-8"))
                self.predecessor = (message[1][0], message[1][1])
                break

            elif message_type == "setSuccessor":
                # set successor and second_successor
                self.successor = (message[1][0], message[1][1])
                self.second_successor = (message[2][0], message[2][1])

                # connect to predecessor to update its second_successor
                sender = socket.create_connection(self.predecessor)
                sender.recv(1024)
                to_send = ["setSecondSuccessor", self.successor]
                sender.send(json.dumps(to_send).encode("utf-8"))
                break

            elif message_type == "put":
                # receive and save file
                client.send(json.dumps(["start"]).encode("utf-8"))
                filename = message[1]
                self.files.append(filename)
                directory = self.host +"_"+str(self.port)+"/"+filename
                self.recieveFile(client, directory)
                client.send(json.dumps(["done"]).encode("utf-8"))

                # create a backup of the file at successor
                sender = socket.create_connection(self.successor)
                sender.recv(1024)
                to_send = ["backUp", filename]
                sender.send(json.dumps(to_send).encode("utf-8"))
                sender.recv(1024)
                self.sendFile(sender, filename)
                sender.recv(1024)
                break

            elif message_type == "get":
                filename = message[1]
                if filename in self.files:
                    client.send(json.dumps(["found"]).encode("utf-8"))
                    client.recv(1024)
                    directory = self.host+"_"+str(self.port)+"/"+filename
                    self.sendFile(client, directory)
                else:
                    # if file isnt in system
                    client.send(json.dumps(["notFound"]).encode("utf-8"))

                break

            elif message_type == "fileTransferOnJoin":
                # send the new node its share of files
                to_delete = []
                pre_key = message[1]
                client.send(json.dumps(["sending"]).encode("utf-8"))
                client.recv(1024)
                for file in self.files:
                    key = self.hasher(file)
                    normal = pre_key < self.key and (key < pre_key or key > self.key)
                    corner_case = pre_key > self.key and self.key < key < pre_key
                    if normal or corner_case:
                        to_delete.append(file)
                        to_send = ["file", file]
                        client.send(json.dumps(to_send).encode("utf-8"))
                        client.recv(1024)
                        directory = self.host+"_"+str(self.port)+"/"+file
                        self.sendFile(client, directory)
                        client.recv(1024)
                client.send(json.dumps(["end"]).encode("utf-8"))
                to_backup = []
                for file in to_delete:
                    # remove from own files
                    self.files.remove(file)
                    # store for backing up
                    to_backup.append(file)

                # send the previous predecessor's backUpFiles to the new node
                sender = socket.create_connection(self.predecessor)
                sender.recv(1024)
                sender.send(json.dumps(["receiveBackUp"]).encode("utf-8"))
                sender.recv(1024)
                for file in self.backUpFiles:
                    to_send = ["file", file]
                    sender.send(json.dumps(to_send).encode("utf-8"))
                    sender.recv(1024)
                    directory = self.host+"_"+str(self.port)+"/"+file
                    self.sendFile(sender, directory)
                    sender.recv(1024)
                sender.send(json.dumps(["end"]).encode("utf-8"))

                # put files from to_backup into self.backUpFiles
                self.backUpFiles = []
                for file in to_backup:
                    self.backUpFiles.append(file)
                break

            elif message_type == "predecessorLeft":
                # send self.successor for updating second_successor
                client.send(json.dumps(["setSecondSuccessor", self.successor]).encode("utf-8"))
                directory = self.host+"_"+str(self.port)+"/"
                # update predecessor
                self.predecessor = (message[1][0], message[1][1])

                sender = socket.create_connection(self.successor)
                sender.recv(1024)
                sender.send(json.dumps(["receiveBackUp"]).encode("utf-8"))
                sender.recv(1024)
                for file in self.backUpFiles:
                    # move files from backUpFiles to self.files
                    self.files.append(file)
                    to_send = ["file", file]
                    sender.send(json.dumps(to_send).encode("utf-8"))
                    sender.recv(1024)
                    directory = self.host+"_"+str(self.port)+"/"+file
                    # send these files to successor for backUp
                    self.sendFile(sender, directory)
                    sender.recv(1024)
                sender.send(json.dumps(["end"]).encode("utf-8"))

                self.backUpFiles = []

                # receive predecessor's files for backUp after failure of its successor
                sender = socket.create_connection(self.predecessor)
                while True:
                    msg = sender.recv(1024)
                    msg = msg.decode("utf-8")
                    msg = json.loads(msg)
                    msg_type = msg[0]

                    if msg_type == "connected":
                        to_send = ["sendBackUp"]
                        sender.send(json.dumps(to_send).encode("utf-8"))
                    elif msg_type == "sending":
                        to_send = ["start"]
                        sender.send(json.dumps(to_send).encode("utf-8"))
                    elif msg_type == "file":
                        filename = msg[1]
                        self.backUpFiles.append(filename)
                        directory = self.host+"_"+str(self.port)+"/"+filename
                        to_send = ["start"]
                        sender.send(json.dumps(to_send).encode("utf-8"))
                        self.recieveFile(sender, directory)
                        to_send = ["done"]
                        sender.send(json.dumps(to_send).encode("utf-8"))
                    elif msg_type == "end":
                        break
                break

            elif message_type == "setSecondSuccessor":
                self.second_successor = (message[1][0], message[1][1])
                break

            elif message_type == "backUp":
                # backUp new file after put is called at predecessor
                client.send(json.dumps(["start"]).encode("utf-8"))
                filename = message[1]
                self.backUpFiles.append(filename)
                directory = self.host +"_"+str(self.port)+"/"+filename
                self.recieveFile(client, directory)
                client.send(json.dumps(["done"]).encode("utf-8"))
                break

            elif message_type == "receiveBackUp":
                client.send(json.dumps(["startTransfer"]).encode("utf-8"))
                while True:
                    msg = client.recv(1024)
                    msg = msg.decode("utf-8")
                    msg = json.loads(msg)
                    msg_type = msg[0]

                    if msg_type == "file":
                        filename = msg[1]
                        self.backUpFiles.append(filename)
                        directory = self.host+"_"+str(self.port)+"/"+filename
                        to_send = ["start"]
                        client.send(json.dumps(to_send).encode("utf-8"))
                        self.recieveFile(client, directory)
                        to_send = ["done"]
                        client.send(json.dumps(to_send).encode("utf-8"))
                    elif msg_type == "end":
                        break
                break

            elif message_type == "sendBackUp":
                client.send(json.dumps(["sending"]).encode("utf-8"))
                client.recv(1024)
                for file in self.files:
                    to_send = ["file", file]
                    client.send(json.dumps(to_send).encode("utf-8"))
                    client.recv(1024)
                    directory = self.host+"_"+str(self.port)+"/"+file
                    self.sendFile(client, directory)
                    client.recv(1024)
                client.send(json.dumps(["end"]).encode("utf-8"))
                break

            elif message_type == "ping":
                break

    def listener(self):
        '''
        Every inbound connection, spins a new thread in the form of handleConnection function.
        '''
        listener = socket.socket()
        listener.bind((self.host, self.port))
        listener.listen(10)
        while not self.stop:
            client, addr = listener.accept()
            threading.Thread(target=self.handleConnection, args=(client, addr)).start()
        print("Shutting down node:", self.host, self.port)
        try:
            listener.shutdown(2)
            listener.close()
        except:
            listener.close()

    def join(self, joiningAddr):
        '''
        Handles the logic of a node joining.
        '''
        if not joiningAddr: # if single node
            return
        sender = socket.create_connection(joiningAddr)
        while True:
            msg = json.loads(sender.recv(1024).decode("utf-8"))
            msg_type = msg[0]

            if msg_type == "connected":
                to_send = ["join", self.key, (self.host, self.port)]
                sender.send(json.dumps(to_send).encode("utf-8"))
            elif msg_type == "twoNodeSystem":
                # Two nodes corner case
                self.successor = joiningAddr
                self.predecessor = joiningAddr
                self.pinging.start()
                return
            elif msg_type == "setSuccessor":
                # General case
                self.successor = (msg[1][0], msg[1][1])
                break

        # connect with successor and update its predecessor
        sender = socket.create_connection(self.successor)
        sender.recv(1024)
        to_send = ["setPredecessor", (self.host, self.port)]
        sender.send(json.dumps(to_send).encode("utf-8"))
        msg = json.loads(sender.recv(1024).decode("utf-8"))
        # update predecessor and second_successor based on successor's response
        self.predecessor = (msg[1][0], msg[1][1])
        self.second_successor = (msg[2][0], msg[2][1])

        # connect with predecessor
        sender = socket.create_connection(self.predecessor)
        sender.recv(1024)
        # update its successor and second_successor
        to_send = ["setSuccessor", (self.host, self.port), self.successor]
        sender.send(json.dumps(to_send).encode("utf-8"))

        # get my share of the files from my successor
        sender = socket.create_connection(self.successor)
        while True:
            msg = json.loads(sender.recv(1024).decode("utf-8"))
            msg_type = msg[0]

            if msg_type == "connected":
                to_send = ["fileTransferOnJoin", self.key]
                sender.send(json.dumps(to_send).encode("utf-8"))
            elif msg_type == "sending":
                to_send = ["start"]
                sender.send(json.dumps(to_send).encode("utf-8"))
            elif msg_type == "file":
                filename = msg[1]
                self.files.append(filename)
                directory = self.host+"_"+str(self.port)+"/"+filename
                to_send = ["start"]
                sender.send(json.dumps(to_send).encode("utf-8"))
                self.recieveFile(sender, directory)
                to_send = ["done"]
                sender.send(json.dumps(to_send).encode("utf-8"))
            elif msg_type == "end":
                break

        # start periodic pinging to detect potential failures
        self.pinging.start()

    def lookup(self, key):
        '''
        Finds the node responsible for a given key.
        '''
        # get successor's key
        sender = socket.create_connection(self.successor)
        sender.recv(1024)
        sender.send(json.dumps(["getKey"]).encode("utf-8"))
        msg = json.loads(sender.recv(1024).decode("utf-8"))
        successor_key = msg[1]

        if successor_key > key > self.key:
            # if key lies between my key and successor's key -> maps to successor
            return self.successor
        elif successor_key < self.key and (key > self.key or key < successor_key):
            # end of ring corner case
            return self.successor
        else:
            # forward the lookup request to successor
            sender = socket.socket()
            sender.connect(self.successor)
            sender.recv(1024)
            to_send = ["lookUpRequest", key]
            sender.send(json.dumps(to_send).encode("utf-8"))
            msg = json.loads(sender.recv(1024).decode("utf-8"))
            return msg[1]

    def periodic_pinging(self):
        '''
        Periodically pings successor to detect possible failures.
        '''
        while not self.stop:
            counter = 0
            while not self.stop:
                sender = socket.socket()
                # set a timeout for detection
                sender.settimeout(0.05)
                try:
                    sender.connect(self.successor)
                    sender.recv(1024)
                    sender.send(json.dumps(["ping"]).encode("utf-8"))
                    counter = 0
                    time.sleep(0.5)
                except:
                    counter += 1
                    if counter == 3:
                        # successor node is down
                        break
                    time.sleep(0.25)

            if self.stop:
                break

            # update successor to second_successor
            self.successor = self.second_successor
            # inform successor of failure of its predecessor
            sender = socket.create_connection(self.successor)
            sender.recv(1024)
            sender.send(json.dumps(["predecessorLeft", (self.host, self.port)]).encode("utf-8"))
            msg = json.loads(sender.recv(1024).decode("utf-8"))
            self.second_successor = (msg[1][0], msg[1][1])

            # update second successor for predecessor to self.successor
            sender = socket.create_connection(self.predecessor)
            sender.recv(1024)
            sender.send(json.dumps(["setSecondSuccessor", self.successor]).encode("utf-8"))

    def put(self, fileName):
        '''
        Finds the node responsible for a file, and sends the file over the socket to that node.
        '''
        key = self.hasher(fileName)
        # find node responsible for file by doing a lookup
        node = self.lookup(key)
        node = (node[0], node[1])
        # connect with node
        sender = socket.create_connection(node)
        sender.recv(1024)
        # send file
        to_send = ["put", fileName]
        sender.send(json.dumps(to_send).encode("utf-8"))
        sender.recv(1024)
        self.sendFile(sender, fileName)
        sender.recv(1024)

    def get(self, fileName):
        '''
        Finds the node responsible for a file, gets the file from the responsible node, and saves it.
        '''
        key = self.hasher(fileName)
        # find node responsible for the file by doing a lookup
        node = self.lookup(key)
        node = (node[0], node[1])

        # get file from the node
        sender = socket.create_connection(node)
        while True:
            msg = sender.recv(1024)
            msg = msg.decode("utf-8")
            msg = json.loads(msg)
            msg_type = msg[0]

            if msg_type == "connected":
                to_send = ["get", fileName]
                sender.send(json.dumps(to_send).encode("utf-8"))
            elif msg_type == "found":
                to_send = ["start"]
                sender.send(json.dumps(to_send).encode("utf-8"))
                self.recieveFile(sender, "./"+fileName)
                return fileName
            elif msg_type == "notFound":
                return None

    def leave(self):
        '''
        Causes a node to gracefully leave the network.
        '''
        # inform successor
        sender = socket.create_connection(self.successor)
        sender.recv(1024)
        to_send = ["predecessorLeft", self.predecessor]
        sender.send(json.dumps(to_send).encode("utf-8"))

        # update successor and second_successor of predecessor
        sender = socket.create_connection(self.predecessor)
        sender.recv(1024)
        to_send = ["setSuccessor", self.successor, self.second_successor]
        sender.send(json.dumps(to_send).encode("utf-8"))

        # close all threads
        self.stop = True

    def sendFile(self, soc, fileName):
        '''
        Utility function to send a file over a socket
        '''
        fileSize = os.path.getsize(fileName)
        soc.send(str(fileSize).encode('utf-8'))
        soc.recv(1024).decode('utf-8')
        with open(fileName, "rb") as file:
            contentChunk = file.read(1024)
            while contentChunk != "".encode('utf-8'):
                soc.send(contentChunk)
                contentChunk = file.read(1024)

    def recieveFile(self, soc, fileName):
        '''
        Utility function to recieve a file over a socket
        '''
        fileSize = int(soc.recv(1024).decode('utf-8'))
        soc.send("ok".encode('utf-8'))
        contentRecieved = 0
        file = open(fileName, "wb")
        while contentRecieved < fileSize:
            contentChunk = soc.recv(1024)
            contentRecieved += len(contentChunk)
            file.write(contentChunk)
        file.close()

    def kill(self):
        self.stop = True
