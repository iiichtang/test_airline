import time
from datetime import datetime
import time
import csv
from random import randint

test_time = datetime(2015,1,1,10,10)

current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

print current_time


current_time = test_time.strftime('%Y-%m-%dT%H:%M:%SZ')

print current_time

s = "2015/1/2"
current_time = time.mktime(datetime.strptime(s, "%Y/%m/%d").timetuple())
print current_time


f = open('take_flight_specialid.csv', 'r')
ff = open('take_flight_specialid2.csv', 'wb')
w = csv.writer(ff)

tmp_date = 0

for row in csv.reader(f):
    time_stamp = int(time.mktime(datetime.strptime(row[3], "%Y/%m/%d").timetuple()))
    #print tmp_date, time_stamp
    if tmp_date != time_stamp:
        data = "INSERT take_flight,customer_id=%s,depstn=%s,arrstn=%s value=%s %s" % (row[0], row[1], row[2] , randint(0,9), time_stamp)
        tmp_date = time_stamp
    else:
        data = "INSERT take_flight,customer_id=%s,depstn=%s,arrstn=%s value=%s %s" % (row[0], row[1], row[2] , randint(0,9), time_stamp + 50)
        tmp_date = time_stamp + 50
    print data
    w.writerows(data)

f.close()
ff.close()


