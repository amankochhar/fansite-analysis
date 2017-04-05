# Coded by Aman Kochhar
# amanskochhar@gmail.com
# For insight fellowship coding challenge 
# Please read the adjoining readme to see how this works, 
# Also the code is heavily commented to explain what purpose each function serves

# imports 
import sys
import io
import time
import operator
import re

# dicts and list to save the results
feature1 = {}
feature2 = {}
feature3 = {}
feature4 = {}
blocked = []

# feature 5 gives an insight in to the data and the types of requests made to the server.
# This data can help us better manage ur servers to enhance the databases and requests of a certain(most popular type)
feature5 = {}

############################################################################
# 2. GETTING ANSWERS
# this processes each line and gets the answers for all four features
# IN - main(); OUT - writeData()
############################################################################
def answers (line):
	# each line contains 1. host, 2. timestamp 3. request 4. bytes or '-'
	# using regex to get the data from each line
	print(line)
	temp = line.rstrip('\n').split(" ")
	host = (re.search("^\S+", line)).group(0)
	
	timestamp = (re.search("\[(.*?)\]", line)).group(1)
	# modifying for feature 3
	busiestHour = re.sub(":[0-9]+:[0-9]\w ", ":00:00 ", timestamp)
	# stripping time for feature 4
	tempTime = timestamp.split(" ")
	timestamp = time.strptime(tempTime[0], "%d/%b/%Y:%H:%M:%S")
	timestamp =  time.mktime(timestamp)

	request = (re.search("\"(.*?)\"", line))
	print(request)
	request = request.group(1).split(" ") if len(request.group(1).split(" ")) > 2 else [0,0,0]
	requestType = request[0]
	requestedResource = request[1]
	#security = request[2]
	HTTPreply = temp[-2]
	bytesSend = 0 if temp[-1] == "-" else temp[-1]
	#print(host, timestamp, requestType, requestedResource, requestedResource, HTTPreply, bytesSend) 

	feature1[host] = feature1.get(host, 0) +1
	if requestedResource != 0:
		feature2[requestedResource] = feature2.get(requestedResource, 0) + int(bytesSend)
	feature3[busiestHour] = feature3.get(busiestHour, 0) + 1
	
	# for feature 4 first we check if the reply is 401
	if HTTPreply == "401":
		# next we check if this host is already in our watchlist
		if host in feature4:
			if len(feature4[host]) == 1:
				feature4[host].append(timestamp)
			elif len(feature4[host]) == 3:
				diff = abs(feature4[host][-1] - timestamp)
				if diff <= 300:
					blocked.append(line.rstrip('\n'))
				else:
					# outside the 5 minute window removing the block
					feature4.pop(host, None)
			elif len(feature4[host]) == 2:
				secondTime = feature4[host][1]
				diff = abs(secondTime - timestamp)
				if diff < 20:
					firstTime = feature4[host][0]
					diff = abs(firstTime - timestamp)
					if diff < 20:
						feature4[host].append(timestamp)
					else:
						feature4[host] = feature4[host][-2]
				else:
					feature4.append(timestamp)
			
		else: #just add the host with the time 
			feature4[host] = [timestamp]

	if HTTPreply == "200" and host in feature4:
		if len(feature4[host]) < 3:
			feature4.pop(host, None)
		else:
			diff = abs(feature4[host][-1] - timestamp)
			if diff <= 300:
				blocked.append(line.rstrip('\n'))
			else:
				# outside the 5 minute window removing the block
				feature4.pop(host, None)

	feature5[requestType] = feature5.get(requestType, 0) + 1
	
############################################################################
# 3. WRITING DATA
# this writes median degree in to output.txt
# IN - medianDegree(); OUT - output(s).txt
############################################################################
def writeData(outputPath, writeThis):
	output = open(outputPath, 'a')
	if(len(writeThis[0]) == 2):
		for i in writeThis:
			temp = str(i[0])+","+str(i[1])
			output.write(temp)
			output.write("\n")
	else:
		for i in writeThis:
			output.write(str(i))
			output.write("\n")
	output.close()
	
############################################################################
# 1. READING DATA/ PREPPING OUTPUT(blocked, hosts, hours, resources).TXT
# the program starts here
# this preps the output(s).txt and reads the files and sends it for further processing
# IN - log.txt files; OUT - def answers()
############################################################################
# to clean the previous output file and make sure the program runs on both the shell
# and on the normal execution of the code 
def main():
	if len(sys.argv) != 6:
		print("Please provide the path for input file and four output files. Or execute the run.sh")
		sys.exit(1)

	IP1 = sys.argv[1]
	OP1 = sys.argv[2]
	OP2 = sys.argv[3]
	OP3 = sys.argv[4]
	OP4 = sys.argv[5]

	# to work on both windows and linux/unix systems
	try:
		output = open(OP1, 'w')
		output = open(OP2, 'w')
		output = open(OP3, 'w')
		output = open(OP4, 'w')
		output.close()
	except:
		os.chdir("../")
		output = open(OP1, 'w')
		output = open(OP2, 'w')
		output = open(OP3, 'w')
		output = open(OP4, 'w')
		output.close()
		
	# open the input file and send lines to answers()
	with io.open(IP1, 'r', encoding = "ascii", errors = "ignore") as fileToRead:
	    for line in fileToRead:
	    	answers(line)
	fileToRead.close()

	sorted_feature1 = sorted(feature1.items(), key=operator.itemgetter(1), reverse = True)
	sorted_feature2 = sorted(feature2.items(), key=operator.itemgetter(1), reverse = True)
	sorted_feature3 = sorted(feature3.items(), key=operator.itemgetter(1), reverse = True)
	sorted_feature5 = sorted(feature5.items(), key=operator.itemgetter(1), reverse = True)


	print("Top 10 most active host/IP addresses are: \n", sorted_feature1[:10], "\n")
	print("Top 10 resources that consume the most bandwidth are: \n", ([x[0] for x in sorted_feature2[:10]]), "\n")
	print("Top 10 busiest hours are: \n", sorted_feature3[:10], "\n")
	print("Blocked accesses are: \n", blocked[:10], "\n")
	print("Total number of blocked accesses are: ", len(blocked), "\n")

	totalRequests = sum(feature5.values())
	print("Total number of requests made to the server are: ", totalRequests)
	print("Out of these ", sorted_feature5[0][1], " were: ", sorted_feature5[0][0], " requests, which is ", "{0:.2f}".format((int(sorted_feature5[0][1])/totalRequests)*100) ,"% of the total")


	# writing the results to the output files
	writeData(OP1, sorted_feature1[:10])
	writeData(OP2, ([x[0] for x in sorted_feature2[:10]]))
	writeData(OP3, sorted_feature3[:10])
	writeData(OP4, blocked)

if __name__ == '__main__':
	main()
