import socket
import threading
import os
import sys
import pickle

TCP_IP = "127.0.0.1"
ValidCommands = ["send","insert", "delete", "get", "update", "delay", "search", "show-all"]
GlobalFlags = {"keep_accepting": True, "keep_reading": True}

class message:
	def __init__(self):
		self.message = ""
		self.delay = 0
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
		if l[0] == argv[2]:
			p = serverInit(int(l[1]))
		sockets[l[0]] = int(l[1])
	f.close()

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
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect((TCP_IP, sockets[argv[2]]))
			s.close()
		elif c[0] in ValidCommands:
			if c[0] == "send":
				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				s.connect((TCP_IP, sockets[c[1]]))
				s.send(c[2])
				s.close()
		else:
			print "invalid command, try again"
	acc.join()


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
	print req

if __name__ == "__main__":
	main(sys.argv)
