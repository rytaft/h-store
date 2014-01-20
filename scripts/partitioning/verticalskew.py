#! /usr/bin/python

import math

####

def sinewave(steps, max_factor, frequency = 2.0):
	"""
	Produces sinewave vertical skew. Parameters:
	- number of steps
	- maximum load increase factor
	- number of peaks (optional)
	"""
	res = []
	max_factor = float(max_factor)
	frequency = float(frequency)
	steps = int(steps)
	if max_factor < 1:
		print "ERROR: max_factor must be greater than one"
		exit(1)
	if steps < 1:
		print "ERROR: steps must be greater than one"
		exit(1)
	if frequency < 1:
		print "ERROR: frequency must be greater than one"
		exit(1)

	for current in range(steps):
		ampl = (max_factor - 1.0) / 2.0
		radians = math.pi * 2 * (frequency * float(current)/float(steps))
		res.append(ampl * math.sin(radians + 1.5 * math.pi) + ampl + 1.0)
		
	return res
	
####
	
def linear(steps, max_factor):
	"""
	Produces linear vertical skew. Parameters:
	- number of steps
	- maximum load increase factor
	"""
	res = []
	max_factor = float(max_factor)
	steps = int(steps)
	if max_factor < 1:
		print "ERROR: max_factor must be greater than one"
		exit(1)
	if steps < 1:
		print "ERROR: steps must be greater than one"
		exit(1)

	increment = (max_factor - 1.0) / float(steps)
	for current in range(steps):
		res.append(1.0 + float(current) * increment);
		
	return res
	
####

def spike(steps, max_factor, period, duration):
	"""
	Produces spike vertical skew. Parameters:
	- number of steps
	- maximum load increase factor
	- spike period
	- spike duration
	"""
	res = []
	max_factor = float(max_factor)
	period = int(period)
	duration = int(duration)
	steps = int(steps)
	if max_factor < 1:
		print "ERROR: max_factor must be greater than one"
		exit(1)
	if steps < 1:
		print "ERROR: steps must be greater than one"
		exit(1)
	if duration < 1:
		print "ERROR: duration must be greater than one"
		exit(1)
	if period >= steps:
		print "ERROR: period must be smaller than steps"
		exit(1)

	in_spike = False
	for current in range(1,steps+1):
		if not in_spike and current % period == 0:
			in_spike = True
			spike_started = current
			res.append(max_factor)
		elif in_spike and (current - spike_started) < duration:
			res.append(max_factor)
		elif in_spike and (current - spike_started) >= duration:
			in_spike = False
			res.append(1.0)
		else:
			res.append(1.0)
		
	return res
	
####

if __name__ == "__main__":
	import sys
	
	algos = ["sinewave", "spike", "linear", "linear+sinewave", "linear+spike"]
	if (len(sys.argv) <= 1):
		print "\nPossible vertical skews:",
		for a in algos:
			print(a),
		print "\nEnter skew name to get list of paramters\n"
		exit(0)
		
	if (sys.argv[1] == "sinewave"):
		if (len(sys.argv) <= 3):
			print sinewave.__doc__
			exit(0)
		if (len(sys.argv) <= 4):
			list_inc=sinewave(int(sys.argv[2]), float(sys.argv[3]))
		else:
			list_inc=sinewave(int(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4]))
			
	elif (sys.argv[1] == "linear"):
		if (len(sys.argv) <= 3):
			print linear.__doc__
			exit(0)
		list_inc=linear(int(sys.argv[2]), float(sys.argv[3]))
		
	elif (sys.argv[1] == "spike"):
		if (len(sys.argv) <= 3):
			print spike.__doc__
			exit(0)
		list_inc=spike(int(sys.argv[2]), float(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]))

	elif (sys.argv[1] == "linear+sinewave"):
		if (len(sys.argv) <= 3):
			print """
			Combination of linear and sinewave. Parameters:
			- number of steps
			- maximum LINEAR load increase factor
			- maximum SINEWAVE load increase factor
			- number of peaks (optional)
			"""
			exit(0)
		list_lin = linear(int(sys.argv[2]), float(sys.argv[3]))
		if (len(sys.argv) <= 5):
			list_inc = sinewave(int(sys.argv[2]),float(sys.argv[4]))
		else:
			list_inc = sinewave(int(sys.argv[2]),float(sys.argv[4]),float(sys.argv[5]))
		for i in range(len(list_inc)):
			list_inc[i] += list_lin[i] - 1
			
	elif (sys.argv[1] == "linear+spike"):
		if (len(sys.argv) <= 3):
			print """
			Combination of linear and spike. Parameters:
			- number of steps
			- maximum LINEAR load increase factor
			- maximum SPIKE load increase factor
			- spike period
			- spike duration
			"""
			exit(0)
		list_lin = linear(int(sys.argv[2]), float(sys.argv[3]))
		list_inc = spike(int(sys.argv[2]), float(sys.argv[4]), int(sys.argv[5]),int(sys.argv[6]))
		for i in range(len(list_inc)):
			list_inc[i] += list_lin[i] - 1
			
	for l in list_inc:
		print l
