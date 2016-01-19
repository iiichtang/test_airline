import csv


def combine_all_value(write_filename):
    """
    combine all all_value_*.csv files (0 to 19) into one complete list and eliminate the header.
    :param write_filename: the file name you wish to write
    :return: None
    """
    write_ptr = open(write_filename, "w")

    for count in range(20):
        read_filename = "all_value_%s.csv" % count
        read_ptr = open(read_filename, "r")

        for line in read_ptr:
            if line[0] == 'i':
                print count
                continue
            write_ptr.write(line)

        read_ptr.close()

    write_ptr.close()


def gap_filter(read_filename, write_filename):
    read_ptr = open(read_filename, "r")
    write_ptr = open(write_filename, 'wb')
    wr = csv.writer(write_ptr, dialect='excel')

    count = 0

    for row in csv.reader(read_ptr):
        if row[1] == "-1" and row[2] == "-1" and row[3] == "-1":
            # print "gap -1"
            count += 1
            continue
        wr.writerow(row)

    print "eliminate %s no gap data" % count
    read_ptr.close()
    write_ptr.close()


"""
        0  id,
        1  travel_gap_2013,
        2  travel_gap_2014,
        3  travel_gap_2015,
        4  travel_gap,
        5  booking_count
        6  frequency_count_2013,
        7  frequency_count_2014,
        8  frequency_count_2015,
        9  frequency_count
        10 revenue_count_2013,
        11 revenue_count_2014,
        12 revenue_count_2015,
        13 revenue_count
        14 duty_free_count_2013,
        15 duty_free_count_2014,
        16 duty_free_count_2015,
        17 duty_free_count
"""


def duty_free_filter(read_filename, write_filename):
    read_ptr = open(read_filename, "r")
    write_ptr = open(write_filename, 'wb')
    wr = csv.writer(write_ptr, dialect='excel')

    count = 0

    for row in csv.reader(read_ptr):
        if row[14] == "0" and row[15] == "0" and row[16] == "0":
            # print "gap -1"
            count += 1
            continue
        wr.writerow(row)

    print "eliminate %s no duty free data" % count
    read_ptr.close()
    write_ptr.close()


if __name__ == "__main__":
    # combine_all_value("all_value.csv")
    gap_filter("all_value.csv", "01_all_value_with_gap.csv")
    duty_free_filter("all_value.csv", "02_all_value_with_duty_free.csv")
    duty_free_filter("01_all_value_with_gap.csv", "03_all_value_with_gap_and_duty_free.csv")
