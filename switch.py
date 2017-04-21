#!/usr/bin/python
# -*- conding=utf-8 -*-

class switch(object):
	def __init__(self, value):
		self.value = value
		self.fall = False

	def __iter__(self):
		yield self.match
		raise StopIteration
	
	def match(self, *args):
		if self.fall or not args:
			return True
		elif self.value in args:
			self.fall = True
			return True
		else:
			return False

def main():
	v = 'ten'
	for case in switch(v):
		if case('one'):
			print 1
			break
		if case('two'):
			print 2
			break
		if case('ten'):
			print 10
			break
		if case():
			print 'something else'
			
if __name__ == '__main__':
	main()
