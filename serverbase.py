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
GlobalVariables = {}
HoldbackQueues = {}
Semaphores = {}

class message:
	def __init__(self):
		self.message = ""
		self.sender = ""
		self.delay = 0.0
		self.time = 0.0



def serverInit(port):
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind((TCP_IP, port))
	s.listen(3)
	return s

def main(argv):
	print "starting"
	f = open(argv[1], 'r')
	sockets = dict()
	for line in f:
		l = line.split()
		if l[0] == "delay":
			GlobalVariables["delay"] = l[1]
		elif l[0] == argv[2]:
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
		command = raw_input("please enter new command:")
		c = command.split()
		if command == "":
			continue
		if command == "exit":
			GlobalFlags["keep_reading"] = False
			GlobalFlags["keep_accepting"] = False
			GlobalFlags["keep_delivering"] = False
			#unhook accept thread
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect((TCP_IP, sockets[argv[2]]))
			s.close()
			#unhook deliver thread
			Semaphores["s1"].release()
			Semaphores["s2"].release()
			Semaphores["s3"].release()
			Semaphores["s4"].release()
		elif c[0] in ValidCommands:
			if c[0] == "send":
				mp1_send(sockets[c[1]], c[2], GlobalVariables["delay"], argv[2])
		else:
			print "invalid command, try again"
	acc.join()
	print "accept closed"
	hq1.join()
	print "hq1 closed"
	hq2.join()
	print "hq2 closed"
	hq3.join()
	print "hq3 closed"
	hq4.join()
	print "hq4 closed"


def mp1_send(serverport, mesg, max_delay, sndr):
	m = message()
	m.message = mesg
	m.delay = random.uniform(0, int(max_delay))
	m.sender = sndr
	m.time = time.time()
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
	print "pushed message"
	Semaphores[mesg_object.sender].release()

def deliverHandler(sender):
	while GlobalFlags["keep_delivering"]:
		Semaphores[sender].acquire()
		try:
			mesg_object = HoldbackQueues[sender].get(False)
			print "message received"
			systime = time.time()
			if mesg_object.time+mesg_object.delay < systime:
				print str(mesg_object.time+mesg_object.delay)+"is greater than "+str(systime)+": delivering message...\n"+mesg_object.message
			else:
				time.sleep(mesg_object.time+mesg_object.delay-systime)
				print mesg_object.message
		except Queue.Empty:
			pass


if __name__ == "__main__":
	main(sys.argv)
