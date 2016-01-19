from base_database import *
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
import datetime
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

    def get_customer_list(self, filename):
        self.user_list = []
        read_ptr = open(filename, "r")
        for row in csv.reader(read_ptr):
            # print row[0]
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
        # print "flight_record_list: " + str(response)

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

        session.close()

        result = {"frequency_count_2013": frequency_count_2013, "frequency_count_2014": frequency_count_2014,
                  "frequency_count_2015": frequency_count_2015, "frequency": frequency}
        # print result
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
            return {"gap_2013": -1, "gap_2014": -1, "gap_2015": -1, "gap": -1}

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

        gap_2013 = -1 if count_2013 <= 1 else (total_time_2013 / count_2013).total_seconds()
        gap_2014 = -1 if count_2014 <= 1 else (total_time_2014 / count_2014).total_seconds()
        gap_2015 = -1 if count_2015 <= 1 else (total_time_2015 / count_2015).total_seconds()
        gap = -1 if count <= 1 else (total_time / count).total_seconds()

        result = {"gap_2013": int(gap_2013), "gap_2014": int(gap_2014), "gap_2015": int(gap_2015), "gap": int(gap)}
        # print result
        return result

    def get_booking_count(self, customer_id):
        Session = scoped_session(sessionmaker(bind=self.engine))
        session = Session()

        booking_count = self.session.query(Booking) \
            .filter(Booking.customer_id == customer_id) \
            .count()

        # print "booking_count", booking_count

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
        if revenue_count_2013 is None:
            revenue_count_2013 = 0

        revenue_count_2014 = self.session.query(func.sum(TakeFlight.usdnet).label("result")) \
            .filter(TakeFlight.customer_id == customer_id) \
            .filter(TakeFlight.depdate >= date(2014, 1, 1)) \
            .filter(TakeFlight.depdate < date(2015, 1, 1)) \
            .all()

        revenue_count_2014 = revenue_count_2014[0].result
        if revenue_count_2014 is None:
            revenue_count_2014 = 0

        revenue_count_2015 = self.session.query(func.sum(TakeFlight.usdnet).label("result")) \
            .filter(TakeFlight.customer_id == customer_id) \
            .filter(TakeFlight.depdate >= date(2015, 1, 1)) \
            .all()

        revenue_count_2015 = revenue_count_2015[0].result
        if revenue_count_2015 is None:
            revenue_count_2015 = 0

        revenue_count = revenue_count_2013 + revenue_count_2014 + revenue_count_2015

        result = {"revenue_count_2013": revenue_count_2013, "revenue_count_2014": revenue_count_2014,
                  "revenue_count_2015": revenue_count_2015, "revenue_count": revenue_count}
        # print result
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
        # print result
        session.close()
        return result

    def get_one_value(self, customer_id):
        travel_value = 0
        duty_free_value = 0

        self.get_flight_record(customer_id)
        travel_gap = self.get_travel_gap()
        booking_count = self.get_booking_count(customer_id)
        frequency = self.get_travel_frequency(customer_id)
        revenue = self.get_revenue(customer_id)
        duty_free = self.get_duty_free(customer_id, "all")

        print "id", customer_id
        print "travel_gap", travel_gap
        print "booking_count", booking_count
        print "frequency", frequency
        print "revenue", revenue
        print "duty_free", duty_free

        result = {"travel_value": travel_value, "duty_free_value": duty_free_value}
        # print result
        return result

    def get_all_value(self):
        self.get_customer_list("customer_id_has_revenue.csv")
        write_ptr = open("has_revenue_value.csv", 'w')
        count = 0

        for customer in self.user_list:
            customer_value = self.get_one_value(customer)

            result = "%s,%s,%s\n" % (customer, customer_value['travel_value'], customer_value['duty_free_value'])
            write_ptr.write(result)

            count += 1
            if count == 5:
                write_ptr.close()
                return None

        write_ptr.close()

    def get_all_value_from_csv(self, read_filename, write_filename):
        read_ptr = open(read_filename, 'r')
        write_ptr = open(write_filename, 'w')
        count = 0

        for row in csv.reader(read_ptr):

            if count == 0:
                count += 1
                continue

            print row

            customer_id = row[0]
            travel_gap_2013 = int(row[1])
            travel_gap_2014 = int(row[2])
            travel_gap_2015 = int(row[3])
            travel_gap = int(row[4])
            booking_count = int(row[5])
            frequency_count_2013 = int(row[6])
            frequency_count_2014 = int(row[7])
            frequency_count_2015 = int(row[8])
            frequency_count = int(row[9])
            revenue_count_2013 = float(row[10])
            revenue_count_2014 = float(row[11])
            revenue_count_2015 = float(row[12])
            revenue_count = float(row[13])
            duty_free_count_2013 = float(row[14])
            duty_free_count_2014 = float(row[15])
            duty_free_count_2015 = float(row[16])
            duty_free_count = float(row[17])

            travel_value = 0
            revenue_value = 0
            duty_free_value = 0
            realized_duty_free = 0
            future_duty_free = 0

            gap_2013 = 0 if travel_gap_2013 <= 1 else 86400 / travel_gap_2013 * 0.2
            gap_2014 = 0 if travel_gap_2014 <= 1 else 86400 / travel_gap_2014 * 0.3
            gap_2015 = 0 if travel_gap_2015 <= 1 else 86400 / travel_gap_2015 * 0.5
            gap_value = (gap_2013 + gap_2014 + gap_2015) * 0.2
            booking_value = (booking_count / 0.8) * 0.5
            frequency_value = (
                                  frequency_count_2013 / 1.5 * 0.2 + frequency_count_2014 / 1.5 * 0.3 + frequency_count_2015 / 1.5 * 0.5) * 0.3

            travel_value = gap_value + booking_value + frequency_value

            realized_revenue = (
                                   revenue_count_2013 / 2000 * 0.2 + revenue_count_2014 / 2000 * 0.3 + revenue_count_2015 / 2000 * 0.5) * 0.55

            revenue_2013 = 0 if frequency_count_2013 <= 0 else revenue_count_2013 / frequency_count_2013 * 0.2
            revenue_2014 = 0 if frequency_count_2014 <= 0 else revenue_count_2014 / frequency_count_2014 * 0.3
            revenue_2015 = 0 if frequency_count_2015 <= 0 else revenue_count_2015 / frequency_count_2015 * 0.5
            future_revenue = ((booking_count * (revenue_2013 + revenue_2014 + revenue_2015)) / 300) * 0.45

            revenue_value = realized_revenue + future_revenue

            realized_duty_free = (
                                     duty_free_count_2013 / 200 * 0.2 + duty_free_count_2014 / 200 * 0.3 + duty_free_count_2015 / 200 * 0.5) * 0.55

            duty_free_2013 = 0 if frequency_count_2013 <= 0 else duty_free_count_2013 / frequency_count_2013 * 0.2
            duty_free_2014 = 0 if frequency_count_2014 <= 0 else duty_free_count_2014 / frequency_count_2014 * 0.3
            duty_free_2015 = 0 if frequency_count_2015 <= 0 else duty_free_count_2015 / frequency_count_2015 * 0.5
            future_duty_free = ((booking_count * (duty_free_2013 + duty_free_2014 + duty_free_2015)) / 50) * 0.45

            duty_free_value = realized_duty_free + future_duty_free

            result = "%s,%s,%s,%s\n" % (customer_id, travel_value, revenue_value, duty_free_value)
            write_ptr.write(result)

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

    def get_all_result(self, read_filename, write_filename):
        self.get_customer_list(read_filename)

        write_prt = open(write_filename, 'wb')

        result = "id,travel_gap_2013,travel_gap_2014,travel_gap_2015,travel_gap,booking_count," \
                 "frequency_count_2013,frequency_count_2014,frequency_count_2015,frequency_count," \
                 "revenue_count_2013,revenue_count_2014,revenue_count_2015,revenue_count," \
                 "duty_free_count_2013,duty_free_count_2014,duty_free_count_2015,duty_free_count\n"
        print result
        write_prt.write(result)

        for data in self.user_list:
            self.get_flight_record(data)
            travel_gap = self.get_travel_gap()
            booking_count = self.get_booking_count(data)
            frequency = self.get_travel_frequency(data)
            revenue = self.get_revenue(data)
            duty_free = self.get_duty_free(data, "all")

            result = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (
                data, travel_gap['gap_2013'], travel_gap['gap_2014'], travel_gap['gap_2015'], travel_gap['gap'],
                booking_count, frequency['frequency_count_2013'], frequency['frequency_count_2014'],
                frequency['frequency_count_2015'], frequency['frequency'], revenue['revenue_count_2013'],
                revenue['revenue_count_2014'], revenue['revenue_count_2015'], revenue['revenue_count'],
                duty_free['duty_free_count_2013'], duty_free['duty_free_count_2014'], duty_free['duty_free_count_2015'],
                duty_free['duty_free_count'])
            print result
            write_prt.write(result)

        write_prt.close()


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
    # flight_aggregate.get_duty_free("19890300000711", "all")
    # flight_aggregate.get_one_value("19920400000403")
    # flight_aggregate.get_one_value("19890100000210")

    # flight_aggregate.get_all_result("customer_id_has_revenue.csv", "has_revenue_metadata.csv")
    # flight_aggregate.get_all_value()
    # flight_aggregate.get_all_value_from_csv("has_revenue_metadata.csv", "has_revenue_value.csv")


    for title_count in range(8, 9):
        print title_count
        read_filename = "customer_%s.csv" % title_count
        write_filename = "all_value_%s.csv" % title_count
        flight_aggregate.get_all_result(read_filename, write_filename)
