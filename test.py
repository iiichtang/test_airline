import csv


read_ptr = open('take_flight.csv.txt', 'r')
count = 0
for row in csv.reader(read_ptr):
    if row[10] is "" or row[15] is "":
        count += 1
print count
