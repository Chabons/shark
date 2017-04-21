#!/usr/bin/python
#-*- coding=utf-8 -*-

import zmq
import multiprocessing
import json
import time
import ConfigParser
import sys
import os
import threading
import hashlib
from switch import switch
from SqlManager.SqlManager import SqlManager 

identitys = multiprocessing.Manager().dict()
config_path = 'shark.ini'
workers = list()
tb_tuple = ('TB_MACHINE_LOGIN','TB_VOD_UPDATE','TB_RES_UPDATE')

def tprint(*args):
	if len(args) == 0 :
		return
	time_struct = time.localtime(time.time())
	date = time.strftime("%Y-%m-%d %H:%M:%S", time_struct)
	sys.stdout.write("[%s] " %date) 
	for arg in args:
		sys.stdout.write("%s " %(arg))
	sys.stdout.write("\n")
	sys.stdout.flush()


def readConfig(path, section):
	cf = ConfigParser.ConfigParser()
	cf.read(path)
	options = cf.options(section)
	config = dict()
	for option in options:
		config[option] = cf.get(section, option)
	return config

def updateTheUpdateTable(args):
	sqlmanager = SqlManager()
	if args.has_key('vod_update_version') and args.has_key('vod_update_status'):
		condition = "where mid = '%s' and target_version = '%s'" %(args.get('mid', ''), args['vod_update_version'])
		if not sqlmanager.updateTable(tb_tuple[1], condition, update_status = args['vod_update_status']):
			tprint('worker:%s update vod_update_tb error' %(multiprocessing.current_process().name))	
	if args.has_key('res_update_version') and args.has_key('res_update_status'):
		condition = "where mid = '%s' and target_version = '%s'" %(args.get('mid', ''), args['res_update_version'])
		if not sqlmanager.updateTable(tb_tuple[2], condition, update_status = args['res_update_status']):
			tprint('worker:%s update res_update_tb error' %(multiprocessing.current_process().name))	

def checkCliUpdate(worker, identity, args, config, update_type):
	if config.get('update', 'no') != 'yes':
		tprint('update switch is off,if error please check the configuration')
		return
	sqlmanager = SqlManager()
	table_name =  (update_type == 1 and [tb_tuple[1]] or (update_type == 2 and [tb_tuple[2]] or [None]))[0]
	result = sqlmanager.queryTable(sql = "select * from %s where mid = %s" %(table_name, args['mid']))
	client_version = (update_type == 1 and [args['vod_version']] or (update_type == 2 and [args['res_version']] or [None]))[0]
	update_version = (update_type == 1 and ['vod_update_version'] or (update_type == 2 and ['res_update_version'] or [None]))[0]
	update_status = (update_type == 1 and ['vod_update_status'] or (update_type == 2 and ['res_update_status'] or [None]))[0]

	if result != None and len(result) > 0 and result[0][5] == '1' and result[0][1] == args.get(update_version, None) and int(args.get(update_status, 0)) >= 3:
		condition = "where mid = '%s'" %(args.get('mid', ''))
		if not sqlmanager.updateTable(table_name, condition, enable = 0, update_status = args[update_status]):
			tprint('worker:%s update %s enable error' %(multiprocessing.current_process().name), table_name)
		return

	if result == None or len(result) == 0 or result[0][5] == '0' or result[0][1] == client_version:
		tprint('worker:%s' %(multiprocessing.current_process().name), "identity:%s  need not update %s" %(identity, result))
		return
	if args.has_key(update_version) and args[update_version] == result[0][1] and int(args[update_status]) != 2 :
		tprint('worker:%s' %(multiprocessing.current_process().name), "identity:%s  need not update %s" %(identity, result))
	 	return
	if time.time() - result[0][3] < (int)(config.get('notify_interval', 600)):
		tprint('worker:%s' %(multiprocessing.current_process().name), "identity:%s  need not update for has notify %s" %(identity, result))
		return	
	count = sqlmanager.queryTable(sql = "select count(*) from %s where update_status = 1" %(table_name))
	if count == None or len(count) == 0 or int(count[0][0]) > int(config.get('update_limit' ,4)):
		tprint('worker:%s' %(multiprocessing.current_process().name), "identity:%s  need not update for over limit:%s  %s" %(identity, count[0][0], result))
		return
	
	ret_dict = {'cmd':'update', 'mid':result[0][0], 'target_version':result[0][1], 'update_url':result[0][2]}
	ret_dict['time'] = int(time.time())
	ret_dict['type'] = update_type
	hash_str = "%s%s%s" %(ret_dict['cmd'], ret_dict['mid'], ret_dict['time'])
	hash_str += r"to80Z4U5tm*$!0IC" 
	ret_dict['signature'] = hashlib.sha1(hash_str).hexdigest()
	heartReturn(identity, worker, ret_dict)
	condition = "where mid = '%s'" %(args.get('mid', ''))
	if not sqlmanager.updateTable(table_name, condition, notify_time = int(time.time())):
		tprint('worker:%s update %s notify time error' %(multiprocessing.current_process().name), table_name)	
	

def heartDealHandler(worker, identity, args, config):
	if not isinstance(args, dict):
		return
	sqlmanager = SqlManager()
	condition = "where mid = '%s' and hard_sn = '%s' and device_id = '%s' and control_id = '%s'" %(args.get("mid", ''), args.get('hard_sn', ''), args.get('device_id', ''), args.get('control_id', ''))
	result = sqlmanager.queryTable(sql = "select * from %s %s" %(tb_tuple[0], condition))
	flag = False
	if result != None and len(result) > 0 and identity == args.get('hard_sn', ''):
		insert_dict = args.copy()
		del insert_dict['mid']
		del insert_dict['cmd']
		del insert_dict['hard_sn']
		del insert_dict['device_id']
		del insert_dict['control_id']
		insert_dict['HEARTBEAT_TIME'] = int(time.time())
		flag = sqlmanager.updateTable(tb_tuple[0], condition, **insert_dict)

	ret_dict = {'cmd':'heart_ret'}
	if flag:
		ret_dict['info_code'] = '10000'
		ret_dict['status'] = 1
		ret_dict['heart_interval'] = (int)(config.get("heart_interval", 120))
	else:
		ret_dict['info_code'] = '10001'
		ret_dict['status'] = 0
	heartReturn(identity, worker, ret_dict)
	if not flag:
		return
	updateTheUpdateTable(args)
	checkCliUpdate(worker, identity, args, config, 1)
	checkCliUpdate(worker, identity, args, config, 2)
	
def commandDealHandler(worker, args, identity):
	if not isinstance(args, dict) or not args.has_key('admin_identity'):
		tprint('worker:%s' %(multiprocessing.current_process().name), "cmd_ret(from %s):%s" %(identity, args), "Error:commandDealHandler error")
		return
	heartReturn(args['admin_identity'].encode('ascii'), worker, args)

def clientMsgDealHandler(worker, identity, args, config):
	if not isinstance(args, dict) or not args.has_key('cmd'):
		return
	for case in switch(args['cmd']):
		if case('heart'):
			heartDealHandler(worker, identity, args, config)
			break
		if case('execute'):
			commandDealHandler(worker, args, identity)
			break
		if case(''):
			tprint("Error cmd", args)

def adminPushMsg(worker, args):
	if not args.has_key('vod_identitys') or not isinstance(args['vod_identitys'], list):
		print type(args['vod_identitys'])
		return
	ret_dict = args.copy()
	del ret_dict['vod_identitys']
	for identity in args['vod_identitys']:
		heartReturn(identity.encode('ascii'), worker, ret_dict)
	

def adminMsgDealHandler(worker, identity, args):
	if not isinstance(args, dict) or not args.has_key('cmd') or not args.has_key('vod_identitys'):
		tprint('worker:%s' %(multiprocessing.current_process().name), "cmd_ret(from %s):%s" %(identity, args), "Error:adminMsgDealHandler params error")
		return
	for case in switch(args['cmd']):
		if case('update'):
			adminPushMsg(worker, args)	
			break
		if case('execute'):
			adminPushMsg(worker, args)	
			break
		if case(''):
			tprint("Error cmd", args)
	
def heartReturn(identity, worker, args):
	if not isinstance(args, dict):
		return
	try:
		ret_str = json.dumps(args)
		worker.send_multipart([identity, ret_str])
		tprint('worker:%s' %(multiprocessing.current_process().name), "identity:%s" %identity, "ret:%s" %ret_str)
	except Exception, e:
		tprint('worker:%s' %(multiprocessing.current_process().name), "identity:%s" %identity, "Error:heartDealHandler %s" %e)

def clientVerify(identity, worker):
	sqlmanager = SqlManager()
	sql = "select * from %s where hard_sn = '%s' " %(tb_tuple[0], identity)
	result = sqlmanager.queryTable(sql = sql)
	if result == None or len(result) == 0 :
		ret_dict = {'cmd':'heart_ret'}
		ret_dict['info_code'] = '10004'
		ret_dict['status'] = 0
		heartReturn(identity, worker, ret_dict)
		return False;
	return True
	
def workerHandler():
	context = zmq.Context()
	worker = context.socket(zmq.DEALER)	
	worker.connect("ipc://backend.ipc")
	tprint("worker:%s pid:%x started" %(multiprocessing.current_process().name, os.getpid()))
	global identitys
	config_db = readConfig(config_path, 'db')
	config_base = readConfig(config_path, 'baseconf')
	sqlmanager = SqlManager()
	sqlmanager.setConnect(config_db['database'], config_db['host'], config_db['user'], config_db['password'], config_db['charset'], int(config_db['port']))
	while True:
		identity, msg = worker.recv_multipart()
		tprint("Worker:%s recv package from %s" %(multiprocessing.current_process().name, identity), msg)
		identitys.setdefault(identity, dict())
		identitys[identity][time] = (int)(time.time())
		try:
			msg = json.loads(msg)
		except Exception, e:
			tprint("Error:%s" %e)
			continue
		if identity[:5] != "admin" and clientVerify(identity, worker):
			clientMsgDealHandler(worker, identity, msg, config_base)
		else:
			adminMsgDealHandler(worker, identity, msg)
			
	worker.close()
	context.term()

def workerStartHandler(index = None):
	global workers
	worker = multiprocessing.Process(target = workerHandler)
	worker.Daemon = True
	worker.start()
	if index != None:
		workers[index] = worker
	else:
		workers.append(worker)
	print("worker is or not alive", worker.is_alive())

def serverStartHandler():
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


def main():
	server_thread = threading.Thread(target = serverStartHandler)
	server_thread.setDaemon(True)
	server_thread.start()
	config = readConfig(config_path, 'baseconf')
	worker_num = (int)(config.get("workers", 1))
	global workers
	for i in xrange(worker_num):
		workerStartHandler()
	while True:
		time.sleep(1200)
		for worker in workers:
			if not worker.is_alive():
				workerStartHandler(workers.index(worker))
	
if __name__ == '__main__':
	main()
