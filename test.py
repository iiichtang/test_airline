import csv

"""
read_ptr = open('take_flight.csv.txt', 'r')
count = 0
for row in csv.reader(read_ptr):
    if row[10] is "" or row[15] is "":
        count += 1
print count

"""

title_count = 0

read_ptr = open('customer_all.csv', 'r')


filename = "customer_%s.csv" % title_count

write_ptr = open(filename, 'w')

count = 0

for row in csv.reader(read_ptr):
    result = "%s" % row[0]
    write_ptr.write(result)
    count += 1
    if count > 1000:
        write_ptr.close()
        title_count += 1
        filename = "customer_%s.csv" % title_count
        write_ptr = open(filename, 'w')
        count = 0
        pass
    else:
        result = "\n"
        write_ptr.write(result)