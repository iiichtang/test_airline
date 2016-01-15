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
from sqlalchemy.sql import func
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

        result = {"frequency_count_2013": frequency_count_2013, "frequency_count_2014": frequency_count_2014,
                  "frequency_count_2015": frequency_count_2015, "frequency": frequency}
        print result
        return result

    def get_travel_gap(self):
        # the gap between each travel
        start_time_2013 = 0
        end_time_2013 = 0
        total_time_2013 = timedelta(0)
        count_2013 = 0

        start_time_2014 = 0
        end_time_2014 = 0
        total_time_2014 = timedelta(0)
        count_2014 = 0

        start_time_2015 = 0
        end_time_2015 = 0
        total_time_2015 = timedelta(0)
        count_2015 = 0

        start_time = 0
        end_time = 0
        total_time = timedelta(0)
        count = 0

        if len(self.flight_record_list) <= 1:
            return {"gap": -1}

        for data in self.flight_record_list:
            if data['depdate'] < date(2014, 1, 1):
                if end_time_2013 == 0:
                    start_time_2013 = data['depdate']
                    end_time_2013 = start_time_2013
                else:
                    total_time_2013 += data['depdate'] - end_time_2013
                    end_time_2013 = data['depdate']
                    count_2013 += 1
            elif date(2014, 1, 1) <= data['depdate'] < date(2015, 1, 1):
                if end_time_2014 == 0:
                    start_time_2014 = data['depdate']
                    end_time_2014 = start_time_2014
                else:
                    total_time_2014 += data['depdate'] - end_time_2014
                    end_time_2014 = data['depdate']
                    count_2014 += 1
            elif data['depdate'] >= date(2015, 1, 1):
                if end_time_2015 == 0:
                    start_time_2015 = data['depdate']
                    end_time_2015 = start_time_2015
                else:
                    total_time_2015 += data['depdate'] - end_time_2015
                    end_time_2015 = data['depdate']
                    count_2015 += 1

            if end_time == 0:
                start_time = data['depdate']
                end_time = start_time
            else:
                total_time += data['depdate'] - end_time
                end_time = data['depdate']
                count += 1

        gap_2013 = 0 if count_2013 == 0 else total_time_2013 / count_2013
        gap_2014 = 0 if count_2014 == 0 else total_time_2014 / count_2014
        gap_2015 = 0 if count_2015 == 0 else total_time_2015 / count_2015
        gap = 0 if count == 0 else total_time / count

        print "start_time_2013", start_time_2013
        print "end_time_2013", end_time_2013
        print "total_time_2013", total_time_2013
        print "count_2013", count_2013
        print "gap_2013", gap_2013

        print "start_time_2014", start_time_2014
        print "end_time_2014", end_time_2014
        print "total_time_2014", total_time_2014
        print "count_2014", count_2014
        print "gap_2014", gap_2014

        print "start_time_2015", start_time_2015
        print "end_time_2015", end_time_2015
        print "total_time_2015", total_time_2015
        print "count_2015", count_2015
        print "gap_2015", gap_2015

        print "start_time", start_time
        print "end_time", end_time
        print "total_time", total_time
        print "count", count
        print "gap", gap

        result = {"gap_2013": gap_2013, "gap_2014": gap_2014, "gap_2015": gap_2015, "gap": gap}
        print result
        return result

    def get_booking_count(self, customer_id):
        Session = scoped_session(sessionmaker(bind=self.engine))
        session = Session()

        booking_count = self.session.query(Booking) \
            .filter(Booking.customer_id == customer_id) \
            .count()

        print "booking_count", booking_count

        session.close()
        return booking_count

    def get_revenue(self, customer_id):
        Session = scoped_session(sessionmaker(bind=self.engine))
        session = Session()

        revenue_count_2013 = self.session.query(func.sum(TakeFlight.usdnet).label("result")) \
            .filter(TakeFlight.customer_id == customer_id) \
            .filter(TakeFlight.depdate < date(2014, 1, 1)) \
            .all()

        revenue_count_2013 = revenue_count_2013[0].result

        revenue_count_2014 = self.session.query(func.sum(TakeFlight.usdnet).label("result")) \
            .filter(TakeFlight.customer_id == customer_id) \
            .filter(TakeFlight.depdate >= date(2014, 1, 1)) \
            .filter(TakeFlight.depdate < date(2015, 1, 1)) \
            .all()

        revenue_count_2014 = revenue_count_2014[0].result

        revenue_count_2015 = self.session.query(func.sum(TakeFlight.usdnet).label("result")) \
            .filter(TakeFlight.customer_id == customer_id) \
            .filter(TakeFlight.depdate >= date(2015, 1, 1)) \
            .all()

        revenue_count_2015 = revenue_count_2015[0].result

        revenue_count = revenue_count_2013 + revenue_count_2014 + revenue_count_2015

        result = {"revenue_count_2013": revenue_count_2013, "revenue_count_2014": revenue_count_2014,
                  "revenue_count_2015": revenue_count_2015, "revenue_count": revenue_count}
        print result
        session.close()
        return result

    def get_duty_free(self, customer_id, method):
        Session = scoped_session(sessionmaker(bind=self.engine))
        session = Session()

        duty_free_count_2013 = 0
        duty_free_count_2014 = 0
        duty_free_count_2015 = 0

        if method != "pay_net_usd":
            count_2013 = self.session.query(func.sum(DutyFree.pay_amt_usd).label("result")) \
                .filter(DutyFree.customer_id == customer_id) \
                .filter(DutyFree.depdate < date(2014, 1, 1)) \
                .all()

            count_2013 = count_2013[0].result
            if count_2013 is None:
                count_2013 = 0

            count_2014 = self.session.query(func.sum(DutyFree.pay_amt_usd).label("result")) \
                .filter(DutyFree.customer_id == customer_id) \
                .filter(DutyFree.depdate >= date(2014, 1, 1)) \
                .filter(DutyFree.depdate < date(2015, 1, 1)) \
                .all()

            count_2014 = count_2014[0].result
            if count_2014 is None:
                count_2014 = 0

            count_2015 = self.session.query(func.sum(DutyFree.pay_amt_usd).label("result")) \
                .filter(DutyFree.customer_id == customer_id) \
                .filter(DutyFree.depdate >= date(2015, 1, 1)) \
                .all()

            count_2015 = count_2015[0].result
            if count_2015 is None:
                count_2015 = 0

            duty_free_count_2013 += count_2013
            duty_free_count_2014 += count_2014
            duty_free_count_2015 += count_2015

        if method != "pay_amt_usd":
            count_2013 = self.session.query(func.sum(DutyFree.pay_net_usd).label("result")) \
                .filter(DutyFree.customer_id == customer_id) \
                .filter(DutyFree.depdate < date(2014, 1, 1)) \
                .all()

            count_2013 = count_2013[0].result
            if count_2013 is None:
                count_2013 = 0

            count_2014 = self.session.query(func.sum(DutyFree.pay_net_usd).label("result")) \
                .filter(DutyFree.customer_id == customer_id) \
                .filter(DutyFree.depdate >= date(2014, 1, 1)) \
                .filter(DutyFree.depdate < date(2015, 1, 1)) \
                .all()

            count_2014 = count_2014[0].result
            if count_2014 is None:
                count_2014 = 0

            count_2015 = self.session.query(func.sum(DutyFree.pay_net_usd).label("result")) \
                .filter(DutyFree.customer_id == customer_id) \
                .filter(DutyFree.depdate >= date(2015, 1, 1)) \
                .all()

            count_2015 = count_2015[0].result
            if count_2015 is None:
                count_2015 = 0

            duty_free_count_2013 += count_2013
            duty_free_count_2014 += count_2014
            duty_free_count_2015 += count_2015

        duty_free_count = duty_free_count_2013 + duty_free_count_2014 + duty_free_count_2015

        result = {"duty_free_count_2013": duty_free_count_2013, "duty_free_count_2014": duty_free_count_2014,
                  "duty_free_count_2015": duty_free_count_2015, "duty_free_count": duty_free_count}
        print result
        session.close()
        return result


if __name__ == "__main__":
    flight_aggregate = flight_aggregator()
    # flight_aggregate.get_customer_list("customer_all.csv")
    # flight_aggregate.get_flight_record("19920400000403")
    # flight_aggregate.get_travel_gap()
    # flight_aggregate.get_booking_count("19920400000403")
    # flight_aggregate.get_travel_frequency("19920400000403")
    # flight_aggregate.get_revenue("19920400000403")
    # flight_aggregate.get_duty_free("19890300000711", "pay_amt_usd")
    # flight_aggregate.get_duty_free("19890300000711", "pay_net_usd")
    flight_aggregate.get_duty_free("19890300000711", "all")
