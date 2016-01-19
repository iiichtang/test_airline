import csv


def gap_filter(read_filename, write_filename):
    read_ptr = open(read_filename, "r")

    resultFile = open(write_filename, 'wb')
    wr = csv.writer(resultFile, dialect='excel')

    for row in csv.reader(read_ptr):
        if row[1] == "-1" and row[2] == "-1" and row[3] == "-1":
            # print "gap -1"
            continue
        wr.writerow(row)


if __name__ == "__main__":
    gap_filter("all_value.csv", "01_all_value_with_gap.csv")
