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
		result = json.loads(result)
		if result['cmd'] == 'execute':
			req = {'cmd':'execute', 'result':'adfasdfasdf'}
			req['admin_identity'] = result['admin_identity']
			socket.send_string(json.dumps(req))


def heart_handler(socket):
	while True:
		req = {'cmd':'heart', 'mid':'1', 'device_id':'LE201609280001', 'hard_sn':'Z5209MCN00000000', 'control_id':'49FF6E065186514846471287', 'vod_version':'kshow-v0.0.1', 'res_version':'kshowRes-v0.0.1', 'ip_internal':'192.168.1.72', 'ip_outside':'114.215.252.192', 'area':'上海' }
		req['vod_update_version'] = 'kshow-v0.0.2'
		req['vod_update_status'] = 2
		socket.send_string(json.dumps(req))
		print('heart_handler')
		time.sleep(5)
		
def main():
	context = zmq.Context()
	socket = context.socket(zmq.DEALER)
	identity = "Z5209MCN00000000"
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
