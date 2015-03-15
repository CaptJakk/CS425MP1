import socket
import threading
import os
import sys
import pickle
import random
import time
import Queue

TCP_IP = "127.0.0.1"
ValidCommands = ["send","insert", "delete", "get", "update", "delay", "search", "show-all"]
GlobalFlags = {"keep_accepting": True, "keep_reading": True, "keep_delivering": True}
GlobalVariables = {"Sequencer_R":1, "Sequencer_S":1}
HoldbackQueues = {}
Semaphores = {}
KeyValueStore = {}
Leader = ""
TO_Holdback = {}
sockets = {}
Name = ""
HoldbackSem = threading.Semaphore(0)
AckSem = threading.Semaphore(0)
EventualQueue = Queue.Queue()

class message:
	def __init__(self):
		self.message = ""
		self.sender = ""
		self.origin = ""
		self.delay = 0.0
		self.time = 0.0
		self.seq = -1



def serverInit(port):
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind((TCP_IP, port))
	s.listen(3)
	return s

def main(argv):
	model = 0
	global Name
	Name = argv[2]
	f = open(argv[1], 'r')
	for line in f:
		l = line.split()
		if "L" in l:
			Leader = l[0]
		if l[0] == "delay":
			GlobalVariables["delay"] = l[1]
		elif l[0] == Name:
			p = serverInit(int(l[1]))
		sockets[l[0]] = int(l[1])
		HoldbackQueues[l[0]] = Queue.Queue()
		Semaphores[l[0]] = threading.Semaphore(0)
	f.close()

	hq1 = threading.Thread(target=deliverHandler, args=("s1",))
	hq2 = threading.Thread(target=deliverHandler, args=("s2",))
	hq3 = threading.Thread(target=deliverHandler, args=("s3",))
	hq4 = threading.Thread(target=deliverHandler, args=("s4",))

	hq1.start()
	hq2.start()
	hq3.start()
	hq4.start()


	print "listening, waiting for connections"
	acc = threading.Thread(target=acceptHandler, args=(p,))
	acc.start()


	while GlobalFlags["keep_reading"]:
		try:
			command = raw_input("please enter new command:")
		except EOFError:
			command = "exit"
		c = command.split()
		if command == "":
			continue
		if command == "exit":
			GlobalFlags["keep_reading"] = False
			GlobalFlags["keep_accepting"] = False
			GlobalFlags["keep_delivering"] = False
			#unhook accept thread
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect((TCP_IP, sockets[Name]))
			s.close()
			#unhook deliver thread
			Semaphores["s1"].release()
			Semaphores["s2"].release()
			Semaphores["s3"].release()
			Semaphores["s4"].release()
			HoldbackSem.release()
			EventualQueue.put("exit")
		elif c[0] in ValidCommands:
			if c[0] == "send":
				mp1_send(sockets[c[1]], c[2], GlobalVariables["delay"], Name, Name, -1)
			if c[0] == "delay":
				time.sleep(float(c[1]))
			if c[0] == "show-all":
				print KeyValueStore
			if c[0] == "search":
				for i in range(4):
					mp1_send(sockets["s"+str(i+1)], command, GlobalVariables["delay"], Name, Name, -1)
			else:
				if len(c) >= 3: #check to see if the model matters
					if model == 0:
						model = int(c[-1])
						comm = threading.Thread(target=commandHandler, args=(model,))
						comm.start()
					if int(c[-1]) == 1: #check to see if the consistency model is one of the first two
						mp1_send(sockets[Leader], command, GlobalVariables["delay"], Name, Name, 0)
					if int(c[-1]) == 2:
						if c[0] == "get":
							try:
								print KeyValueStore[c[1]]
							except KeyError:
								print "Key does not exist"
						else:
							mp1_send(sockets[Leader], command, GlobalVariables["delay"], Name, Name, 0)
					if int(c[-1]) == 3:
						for i in range(4):
							mp1_send(sockets["s"+str(i+1)], command, GlobalVariables["delay"], Name, Name, GlobalVariables["Sequencer_S"])
						TO_Holdback[GlobalVariables["Sequencer_S"]] = 1
						GlobalVariables["Sequencer_S"] += 1
					if int(c[-1]) == 4:
						for i in range(4):
							mp1_send(sockets["s"+str(i+1)], command, GlobalVariables["delay"], Name, Name, GlobalVariables["Sequencer_S"])
						TO_Holdback[GlobalVariables["Sequencer_S"]] = 2
						GlobalVariables["Sequencer_S"] += 1
				else:
					for i in range(4):
						mp1_send(sockets["s"+str(i+1)], command, GlobalVariables["delay"], Name, Name, -1)
		else:
			print "invalid command, try again"
	acc.join()
	hq1.join()
	hq2.join()
	hq3.join()
	hq4.join()
	if model != 0:
		comm.join()

def mp1_send(serverport, mesg, max_delay, sndr, orig, sequence):
	m = message()
	m.message = mesg
	m.delay = random.uniform(0, int(max_delay))
	m.sender = sndr
	m.origin = orig
	m.time = time.time()
	m.seq = sequence
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect((TCP_IP, serverport))
	m_pickled = pickle.dumps(m)
	s.send(m_pickled)
	s.close()

def acceptHandler(sock):
	acceptThreads = []
	while GlobalFlags["keep_accepting"]:
		(conn, addr) = sock.accept()
		t = threading.Thread(target=receiveHandler, args=(conn, addr))
		acceptThreads.append(t)
		t.start()
	for entries in acceptThreads:
		entries.join()
	sock.close()

def receiveHandler(conn, addr):
	req = ""
	while True:
		data = conn.recv(1024);
		if not data: 
			break
		req += data
	if req == "":
		return
	mesg_object = pickle.loads(req)
	HoldbackQueues[mesg_object.sender].put(mesg_object)
	Semaphores[mesg_object.sender].release()

def deliverHandler(sender):
	while GlobalFlags["keep_delivering"]:
		Semaphores[sender].acquire()
		try:
			mesg_object = HoldbackQueues[sender].get(False)
			systime = time.time()
			if mesg_object.time+mesg_object.delay < systime:
				pass
			else:
				time.sleep(mesg_object.time+mesg_object.delay-systime)
			modelHandler(mesg_object)
		except Queue.Empty:
			pass

def modelHandler(mesg_object):
	try:
		c = mesg_object.message.split()
	except AttributeError:
		EventualQueue.put(mesg_object)
	else:
		if c[0] == "delete":
			del KeyValueStore[c[1]]
		if c[0] == "search":
			if c[1] in KeyValueStore:
				mp1_send(sockets[mesg_object.origin], "Result: "+c[1]+" is in server "+Name, GlobalVariables["delay"], Name, Name, -1)
			else:
				mp1_send(sockets[mesg_object.origin], "Result: "+c[1]+" is not in server "+Name, GlobalVariables["delay"], Name, Name, -1)
		if c[0] == "Result:":
			print mesg_object.message
		elif c[0] in ["get", "insert", "update"]:
			if int(c[-1]) in [1, 2]:
				#Total ordering necessary, need to know if leader
				if mesg_object.seq == 0:
					mesg_object.seq = GlobalVariables["Sequencer_S"]
					GlobalVariables["Sequencer_S"] += 1
					for i in range(4):
						mp1_send(sockets["s"+str(i+1)], mesg_object.message, GlobalVariables["delay"], Name, mesg_object.origin, mesg_object.seq)
				else:
					TO_Holdback[mesg_object.seq] = mesg_object
					HoldbackSem.release()
			else:
				EventualQueue.put(mesg_object)
		else:
			EventualQueue.put(mesg_object)

def commandHandler(model):
	if model in [1,2]:
		while GlobalFlags["keep_delivering"]:
			if GlobalVariables["Sequencer_R"] in TO_Holdback:
				mesg = TO_Holdback[GlobalVariables["Sequencer_R"]]
				del TO_Holdback[GlobalVariables["Sequencer_R"]]
				GlobalVariables["Sequencer_R"] += 1
				c = mesg.message.split()
				if c[0] == "get":
					if mesg.origin == Name:
						print KeyValueStore[c[1]]
				elif c[0] in ["insert", "update"]:
					KeyValueStore[c[1]] = c[2]
					if mesg.origin == Name:
						print "ack"
			else:
				HoldbackSem.acquire()
	if model in [3,4]:
		repair = threading.Thread(target=repairHandler, args=())
		repair.start()
		waitingFor = {}
		while GlobalFlags["keep_delivering"]:
			mesg_object = EventualQueue.get()
			if mesg_object == "exit":
				continue
			try:
				c = mesg_object.message.split()
			except AttributeError:
				if mesg_object.message[0] == "repairing":
					if mesg_object.seq in TO_Holdback:
						TO_Holdback[mesg_object.seq] -= 1
						if mesg_object.seq in waitingFor:
							waitingFor[mesg_object.seq].append(mesg_object.message[1])
						else:
							waitingFor[mesg_object.seq] = [mesg_object.message[1]]
						if TO_Holdback[mesg_object.seq] == 0:
							del TO_Holdback[mesg_object.seq]
							recent = (0,0.0)
							for elements in waitingFor[mesg_object.seq]:
								if elements[1] > recent[1]:
									recent = elements
							KeyValueStore[mesg_object.message[2]] = recent
				else:
					if mesg_object.seq in TO_Holdback:
						TO_Holdback[mesg_object.seq] -= 1
						if mesg_object.seq in waitingFor:
							waitingFor[mesg_object.seq].append(mesg_object.message)
						else:
							waitingFor[mesg_object.seq] = [mesg_object.message]
						if TO_Holdback[mesg_object.seq] == 0:
							del TO_Holdback[mesg_object.seq]
							recent = (0,0.0)
							for elements in waitingFor[mesg_object.seq]:
								if elements[1] > recent[1]:
									recent = elements
							print elements[0]

			else:
				if c[0] == "ack":
					if mesg_object.seq in TO_Holdback:
						TO_Holdback[mesg_object.seq] -= 1
						if TO_Holdback[mesg_object.seq] == 0:
							del TO_Holdback[mesg_object.seq]
							print "ack"
				if c[0] == "get":
					if c[1] in KeyValueStore:
						mp1_send(sockets[mesg_object.origin], KeyValueStore[c[1]], GlobalVariables["delay"], Name, Name, mesg_object.seq)
				if c[0] == "repair":
					if c[1] in KeyValueStore:
						mp1_send(sockets[mesg_object.origin], ("repairing",KeyValueStore[c[1]], c[1]), GlobalVariables["delay"], Name, Name, mesg_object.seq)
					else:
						mp1_send(sockets[mesg_object.origin], ("repairing",(0,0.0), None), GlobalVariables["delay"], Name, Name, mesg_object.seq)
				if c[0] in ["insert", "update"]:
					KeyValueStore[c[1]] = (c[2], time.time())
					mp1_send(sockets[mesg_object.origin], "ack", GlobalVariables["delay"], Name, Name, mesg_object.seq)
		repair.join()	
					

def repairHandler():
	while GlobalFlags["keep_delivering"]:
		time.sleep(20)
		for key in KeyValueStore:
			for i in range(4):
				mp1_send(sockets["s"+str(i+1)], "repair "+key+" 3", GlobalVariables["delay"], Name, Name, "r"+str(GlobalVariables["Sequencer_R"]))
			TO_Holdback["r"+str(GlobalVariables["Sequencer_R"])] = 4
			GlobalVariables["Sequencer_R"] += 1


if __name__ == "__main__":
	main(sys.argv)





