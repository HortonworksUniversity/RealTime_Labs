import os,sys,pexpect,random,time,datetime,socket
from time import sleep


if __name__ == "__main__":
  if len(sys.argv) != 3:
    print "Incorrect usage of simulator"
    print "Please pass an integer messages/sec between 1 and 10, followed by a topic"
    print "Pass help to see more information: logSim.py help"
  elif sys.argv[1] == "help":
    print "This simulator exepcts an argument between 1 and 10\nSample Usage : python logSim.py 8 myTopic\nThis will simulate 8 messages per second into 'myTopic'"
  elif int(sys.argv[1]) in range(1,11):
    print "Starting Kafka Broker"
    x = float(1)/float(sys.argv[1])
    print "Printing ",sys.argv[1]," messages per second.  Press control-c to stop"
    sleep(1)
    topic = sys.argv[2]
    toKafka = "/usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh"
    brokers = socket.gethostname() + ":6667"
    z = toKafka + " --broker-list " + brokers + " --topic " + topic
    process = pexpect.spawnu(z)
    data = list(open('logData.txt'))
    while True:
      sleep(x)
      ts = time.time()
      dt = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d\t%H:%M:%S.%f')[:-4]
      ip = str(random.randint(20,60))+"."+str(random.randint(100,300))+"."+str(random.randint(0,999))+"."+str(random.randint(50,400))
      process.sendline(dt+"\t"+ip+"\t"+random.choice(data)[:-1])


exit(0)
