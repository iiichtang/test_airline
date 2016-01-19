import csv

read_ptr = open('customer_id_flight_count.csv', 'r')

count_dict = dict()
count_list = list()
list_index = 0
count_list.append(0)

for row in csv.reader(read_ptr):
    print row
    if row[1] in count_dict:
        count_dict[row[1]] += 1
    else:
        count_dict[row[1]] = 1

print count_dict

for index in range(1, 374):
    if str(index) in count_dict:
        print index
        count_list.append(count_dict[str(index)])
    else:
        count_list.append(0)

print count_list

print len(count_list)
