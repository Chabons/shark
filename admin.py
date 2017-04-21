#!/usr/bin/python
#-*- coding=utf-8 -*-

import zmq
import threading
import time
import json

def thread_handler(socket):
#	poll = zmq.Poller()
#	poll.register(socket, zmq.PLLIN)
	while True:
		result = socket.recv()
		print("result:", result)

def heart_handler(socket):
	while True:
#		req = {'cmd':'update', 'mid':'1', 'type':'2' , 'time':int(time.time()), 'target_version':'sdf', 'update_url':'sadf', 'vod_identitys':['Z5209MCN00000000']}
		req = {'cmd':'execute', 'command':'ls', 'time':int(time.time()), 'admin_identity':'admin', 'signature':'', 'vod_identitys':['Z5209MCN00000000']}
		socket.send_string(json.dumps(req))
		print('update_handler')
		time.sleep(5)
		
def main():
	context = zmq.Context()
	socket = context.socket(zmq.DEALER)
	identity = "admin"
	socket.identity = identity.encode('ascii')
	socket.connect("tcp://localhost:5570")
	print("client %s started" %identity)
	recv_thread = threading.Thread(target = thread_handler, args = (socket,))
	heart_thread = threading.Thread(target = heart_handler, args = (socket,))
	heart_thread.setDaemon(True)
	recv_thread.setDaemon(True)
	recv_thread.start()
	heart_thread.start()
	while True:
		time.sleep(6)

if __name__ == "__main__" :
	main()
