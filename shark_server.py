#!/usr/bin/python
#-*- coding=utf-8 -*-

import zmq
import multiprocessing
import json
import time
import ConfigParser


def main():
	config = readConfig(config_path, 'baseconf')
	context = zmq.Context()
	frontend = context.socket(zmq.ROUTER)
	frontend.bind("tcp://*:5570")
	backend = context.socket(zmq.DEALER)
	backend.bind("ipc://backend.ipc")
	tprint("server started")
	worker_num = (int)(config.get("workers", 1))
	zmq.proxy(frontend, backend)
	frontend.close()
	backend.close()
	context.term()

if __name__ == '__main__':
	main()
