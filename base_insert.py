from base_database import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from datetime import datetime
from datetime import timedelta
import csv

if __name__ == "__main__":
    target_database = 'mysql+pymysql://%s:%s@%s:%s/%s' % (DB_USERNAME, DB_PASSWORD, DB_HOSTNAME,
                                                          DB_PORT, DB_DATABASE)
    engine = create_engine(target_database, echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    read_ptr = open('booking.csv.txt', 'r')
    for row in csv.reader(read_ptr):
        booking_data = Booking(row)
        session.add(booking_data)
    session.commit()
    read_ptr.close()

    read_ptr = open('customer.csv.txt', 'r')
    for row in csv.reader(read_ptr):
        customer_data = Customer(row)
        session.add(customer_data)
    session.commit()
    read_ptr.close()

    read_ptr = open('duty_free.csv.txt', 'r')
    count = 0
    for row in csv.reader(read_ptr):
        if count == 0:
            count += 1
            continue
        duty_free_data = DutyFree(row)
        session.add(duty_free_data)
    session.commit()
    read_ptr.close()

    read_ptr = open('take_flight.csv.txt', 'r')
    row_count = 0
    for row in csv.reader(read_ptr):
        print row
        if row[10] is "" or row[15] is "":
            continue
        take_flight_data = TakeFlight(row)
        session.add(take_flight_data)
        row_count += 1
        if row_count >= 5000:
            session.commit()
            row_count = 0
    read_ptr.close()

    session.close()
