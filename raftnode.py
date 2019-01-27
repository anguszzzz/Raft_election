import rpyc
import sys
import threading
import random
import time
import os
from math import ceil, floor


'''
A RAFT RPC server class.

Please keep the signature of the is_leader() method unchanged (though
implement the body of that function correctly.  You will need to add
other methods to implement ONLY the leader election part of the RAFT
protocol.
'''

class RaftNode(rpyc.Service):
	

	"""
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
	"""
	def __init__(self):
		pass

	'''
        x = is_leader(): returns True or False, depending on whether
        this node is a leader

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call

        CHANGE THIS METHOD TO RETURN THE APPROPRIATE RESPONSE
	'''


	def fol_timeout(self):
		print("timeout")
		self.state = 1
		self.election()

	def send_heartbeat(self):
		print('send heartbeat')
		while True:
			self.lock.acquire()
			self.state = 2
			for k in range(0,len(self.conn)):
				if self.state==0:
					self.lock.release()
					return
				if k!=self.id and self.conn[k]:
					try:
						# if not self.conn[k]:
						# 	try:
						# 		self.conn[k] = rpyc.connect(self.nodes[k][1], self.nodes[k][2])
						# 		print('connect',k)
						# 	except Exception as e:
						# 		continue
						self.lock.release()
						(term, is_valid) = self.conn[k].root.heartbeat(self.term, self.id)
						self.lock.acquire()
						if term>self.term:
							self.term = term
							self.votefor = -1
							with open(self.filename, 'w') as write_out:
								write_out.write(str(self.term) + '\n')
								write_out.write('-1')
							self.state = 0
							self.timer = threading.Timer(self.random_fol_time(), self.fol_timeout)
							self.timer.setDaemon(True)
							self.timer.start()
							self.lock.release()
							return
					except Exception as e:
						print('lose',k)
						self.lock.acquire()
						self.conn[k] = None
			self.lock.release()
			# time.sleep(0.05)

	def exposed_heartbeat(self, term, id):
		self.t1.join()
		print('get_heartbeat',self.term,term)
		self.lock.acquire()
		if term < self.term:
			self.lock.release()
			return (self.term, False)

		else:
			self.state = 0
			if self.timer:
				self.timer.cancel()
			self.timer = threading.Timer(self.random_fol_time(), self.fol_timeout)
			self.timer.setDaemon(True)
			self.timer.start()
			if term>self.term:
				self.term = term
				self.votefor = -1
				with open(self.filename, 'w') as write_out:
					write_out.write(str(self.term) + '\n')
					write_out.write('-1')
			self.lock.release()
			return (self.term, True)

	def election(self):
		while True:
			print('election', self.term)
			tt = threading.Timer(self.random_can_time(), self.can_timeout)
			self.lock.acquire()
			self.term += 1
			self.lock.release()
			self.votefor = -1
			with open(self.filename, 'w') as write_out:
				write_out.write(str(self.term) + '\n')
				write_out.write('-1')
			self.timeout = False
			tt.start()
			index = []
			tempconn = {}
			self.lock.acquire()
			for i in range(0,len(self.conn)):
				tempconn[i] = self.conn[i]
			tempconn.pop(self.id)
			self.lock.release()
			while True:
				vote_count = 1
				self.lock.acquire()
				for i in index:
					tempconn.pop(i)
				index.clear()
				if len(tempconn)>0:
					for k in tempconn:
						if self.state==0:
							self.lock.release()
							return
						if tempconn[k]:
							try:
								# if not tempconn[k]:
								# 	try:
								# 		self.conn[k] = rpyc.connect(self.nodes[k][1], self.nodes[k][2])
								# 		print('connect',k)
								# 		tempconn[k] = self.conn[k]
								# 	except Exception as e:
								# 		continue
								self.lock.release()
								(term, voted) = tempconn[k].root.vote(self.term, self.id)
								self.lock.acquire()
								if term>self.term:
									print("change",self.term,term)
									self.term = term
									self.votefor =-1
									with open(self.filename, 'w') as write_out:
										write_out.write(str(self.term) + '\n')
										write_out.write('-1')
									self.state = 0
									self.timer = threading.Timer(self.random_fol_time(), self.fol_timeout)
									self.timer.setDaemon(True)
									self.timer.start()
									self.lock.release()
									return
								if voted:
									index.append(k)
									vote_count += 1
							except Exception as e:
								print('lose',k)
								self.lock.acquire()
								self.conn[k] = None
								tempconn[k] = None
				# print('vote',vote_count)
				if vote_count >= floor(len(self.nodes)/2) + 1:
					self.lock.release()
					self.send_heartbeat()
					return
				self.lock.release()
				if self.timeout:
					break
				time.sleep(0.05)

	def can_timeout(self):
		self.timeout = True

	def exposed_vote(self, term, id):
		self.t1.join()
		self.lock.acquire()
		if term>self.term:
			self.state = 0
		if self.state == 0:
			if term < self.term or (term==self.term and self.votefor!=-1):
				self.lock.release()
				return (self.term, False)

			else:
				print('timer reset',self.term,term,id)
				if term>self.term:
					self.state = 0
					self.term = term
				self.votefor = id
				self.timer.cancel()
				self.timer = threading.Timer(self.random_fol_time(), self.fol_timeout)
				self.timer.setDaemon(True)
				self.timer.start()
				with open(self.filename, 'w') as write_out:
					write_out.write(str(self.term) + '\n')
					write_out.write(str(id))
				self.lock.release()
				return (self.term, True)
		self.lock.release()
		return (self.term, False)

	def exposed_conn(self, id, ip, port):
		print('conn', id, ip, port)
		while True:
			try:
				self.lock.acquire()
				self.conn[id] = rpyc.connect(ip, port)
				self.lock.release()
				break
			except Exception as e:
				self.lock.release()
				print('no connect')
				pass

	def exposed_is_leader(self):
		return self.state==2

	def random_fol_time(self):
		return (random.random()*500+1000)/1000

	def random_can_time(self):
		return (random.random()*500+1000)/1000

def send_conn():
	pass

def init_server(config, curr_id):
	node.lock = threading.Lock()

	# First try to open the persistent file to find the current term and votedFor
	node.lock.acquire()
	node.term = 0
	node.state = 0  # 0 for follower, 1 for candidate, 2 for leader
	node.votefor = -1  # -1 for not voted yet, others for candidate ID
	node.filename = 'tmp/persistent' + curr_id + '.txt'
	node.timer = None
	node.lock.release()
	if not os.path.exists('tmp'):
		os.mkdir('tmp')

	if os.access(node.filename, os.F_OK):
		with open(node.filename, 'r') as read_in:
			count = 0
			for line in read_in:
				if count == 0:
					node.term = int(line)
				else:
					node.votefor = int(line)
				count += 1

	# Other features
	node.id = int(curr_id)
	with open(config, 'r') as configFile:
		node.total_nodes = configFile.readline()
		node.nodes = []
		for l in configFile:
			words = l.replace('node', '').strip().split(':')
			words[1] = words[1].strip()
			if int(words[0]) == node.id:
				node.ip = words[1]
				node.port = int(words[2])
			node.nodes.append((int(words[0]), words[1], int(words[2])))
	node.conn = []
	for (node_id, node_ip, node_port) in node.nodes:
		if not node_id == node.id:
			try:
				print(node_ip, node_port)
				node.conn.append(rpyc.connect(node_ip, node_port))
				print('connect')
			except Exception as e:
				print('no connect')
				node.conn.append(None)
				pass
		else:
			node.conn.append(None)

	if len(node.conn)>0:
		for i in range(0,len(node.conn)):
			if node.conn[i]:
				try:
					node.conn[i].root.conn(node.id,node.ip,node.port)
				except Exception as e:
					pass
	node.lock.acquire()
	node.timer = threading.Timer(node.random_fol_time(), node.fol_timeout)
	node.timer.setDaemon(True)
	node.timer.start()
	node.lock.release()

if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	node = RaftNode()
	server = ThreadPoolServer(node, port = int(sys.argv[3]))
	t1 = threading.Timer(0.5, init_server, (sys.argv[1], sys.argv[2]))
	node.t1 = t1
	t1.setDaemon(True)
	t1.start()
	server.start()