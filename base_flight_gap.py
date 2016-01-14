from base_database import *
from flask.ext.sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy import Column
from sqlalchemy import Integer, Float
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import DateTime
from sqlalchemy import Date
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
import time
from sqlalchemy import *
from sqlalchemy.orm import *
from datetime import datetime
from datetime import date
from datetime import timedelta
import csv

THRESHOLD_GAP = 0
THRESHOLD_FREQUENCY = 0
THRESHOLD_BOOKING = 0
THRESHOLD_REVENUE = 0
THRESHOLD_FUTURE_REVENUE = 0


class flight_aggregator():
    user_list = list()
    flight_record_list = list()
    target_database = 'mysql+pymysql://%s:%s@%s:%s/%s' % (DB_USERNAME, DB_PASSWORD, DB_HOSTNAME,
                                                          DB_PORT, DB_DATABASE)
    engine = create_engine(target_database, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    def restart_engine(self):
        self.target_database = 'mysql+pymysql://%s:%s@%s:%s/%s' % (DB_USERNAME, DB_PASSWORD, DB_HOSTNAME,
                                                                   DB_PORT, DB_DATABASE)
        self.engine = create_engine(target_database, echo=False)
        self.Session = sessionmaker(bind=engine)
        self.session = Session()

    def get_customer_list(self, filename):
        self.user_list = []
        read_ptr = open(filename, "r")
        for row in csv.reader(read_ptr):
            print row[0]
            self.user_list.append(row[0])

    def get_flight_record(self, customer_id):
        self.flight_record_list = []
        Session = scoped_session(sessionmaker(bind=self.engine))
        session = Session()

        record_list = self.session.query(TakeFlight) \
            .filter(TakeFlight.customer_id == customer_id) \
            .order_by(TakeFlight.depdate).all()

        response = []
        for data in record_list:
            _tmp = data.__dict__
            _tmp.pop('_sa_instance_state', None)
            response.append(_tmp)
        self.flight_record_list = response
        print "flight_record_list: " + str(response)

        session.close()

    def get_travel_frequency(self, customer_id):
        # how many travels per year
        Session = scoped_session(sessionmaker(bind=self.engine))
        session = Session()

        frequency_count_2013 = self.session.query(TakeFlight) \
            .filter(TakeFlight.customer_id == customer_id) \
            .filter(TakeFlight.depdate < date(2014, 1, 1)) \
            .count()

        frequency_count_2014 = self.session.query(TakeFlight) \
            .filter(TakeFlight.customer_id == customer_id) \
            .filter(TakeFlight.depdate >= date(2014, 1, 1)) \
            .filter(TakeFlight.depdate < date(2015, 1, 1)) \
            .count()

        frequency_count_2015 = self.session.query(TakeFlight) \
            .filter(TakeFlight.customer_id == customer_id) \
            .filter(TakeFlight.depdate >= date(2015, 1, 1)) \
            .count()

        frequency = (frequency_count_2013 + frequency_count_2014 + frequency_count_2015) / 3

        print "frequency_count_2013", frequency_count_2013
        print "frequency_count_2014", frequency_count_2014
        print "frequency_count_2015", frequency_count_2015
        print "frequency", frequency

        session.close()
        return {"frequency_count_2013": frequency_count_2013, "frequency_count_2014": frequency_count_2014,
                "frequency_count_2015": frequency_count_2015, "frequency": frequency}

    def get_travel_gap(self):
        # the gap between each travel
        start_time = 0
        end_time = 0
        total_time = timedelta(0)
        count = 0

        if len(self.flight_record_list) <= 1:
            return {"gap": -1}

        for data in self.flight_record_list:
            if end_time == 0:
                start_time = data['depdate']
                end_time = start_time
            else:
                total_time += data['depdate'] - end_time
                end_time = data['depdate']
            count += 1

        print "start_time", start_time
        print "end_time", end_time
        print "total_time", total_time
        print "count", count
        print "gap", total_time / count
        return {"gap": total_time / count}

    def get_booking_count(self, customer_id):
        Session = scoped_session(sessionmaker(bind=self.engine))
        session = Session()

        booking_count = self.session.query(Booking) \
            .filter(Booking.customer_id == customer_id) \
            .count()

        print "booking_count", booking_count

        session.close()
        return booking_count

    def get_flight_usd(self):
        pass

    def get_revenue(self):
        pass

if __name__ == "__main__":
    flight_aggregate = flight_aggregator()
    # flight_aggregate.get_customer_list("customer_all.csv")
    flight_aggregate.get_flight_record("19920400000403")
    flight_aggregate.get_travel_gap()
    # flight_aggregate.get_booking_count("19920400000403")
    flight_aggregate.get_travel_frequency("19920400000403")
