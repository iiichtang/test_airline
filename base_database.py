from sqlalchemy import create_engine
from config import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Time, DateTime, Float, Boolean, TIMESTAMP
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy import ForeignKey
import decimal
from datetime import datetime

Base = declarative_base()


class Customer(Base):
    __tablename__ = 'customer'
    id = Column(String(30, collation='utf8_unicode_ci'), primary_key=True)
    card_type = Column(String(10, collation='utf8_unicode_ci'))
    birthday = Column(Date)
    gender = Column(String(10, collation='utf8_unicode_ci'))
    join_date = Column(Date)
    last_time = Column(Date)
    times_2013 = Column(Integer)
    times_2014 = Column(Integer)
    times_2015 = Column(Integer)
    revenue_2013 = Column(DOUBLE)
    revenue_2014 = Column(DOUBLE)
    revenue_2015 = Column(DOUBLE)
    award_2013 = Column(Integer)
    award_2014 = Column(Integer)
    award_2015 = Column(Integer)
    award_mileage_2013 = Column(Integer)
    award_mileage_2014 = Column(Integer)
    award_mileage_2015 = Column(Integer)
    tour_code_2013 = Column(Integer)
    tour_code_2014 = Column(Integer)
    tour_code_2015 = Column(Integer)
    duty_free_2013 = Column(Integer)
    duty_free_2014 = Column(Integer)
    duty_free_2015 = Column(Integer)

    def __init__(self, data_list):
        self.id = data_list[0]
        self.card_type = data_list[1]
        self.birthday = data_list[2]
        self.gender = data_list[3]
        self.join_date = data_list[4]
        self.last_time = data_list[5]
        self.times_2013 = data_list[6]
        self.times_2014 = data_list[7]
        self.times_2015 = data_list[8]
        self.revenue_2013 = data_list[9]
        self.revenue_2014 = data_list[10]
        self.revenue_2015 = data_list[11]
        self.award_2013 = data_list[12]
        self.award_2014 = data_list[13]
        self.award_2015 = data_list[14]
        self.award_mileage_2013 = data_list[15]
        self.award_mileage_2014 = data_list[16]
        self.award_mileage_2015 = data_list[17]
        self.tour_code_2013 = data_list[18]
        self.tour_code_2014 = data_list[19]
        self.tour_code_2015 = data_list[20]
        self.duty_free_2013 = data_list[21]
        self.duty_free_2014 = data_list[22]
        self.duty_free_2015 = data_list[23]


class Booking(Base):
    __tablename__ = 'booking'
    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(String(30, collation='utf8_unicode_ci'))
    card_type = Column(String(10, collation='utf8_unicode_ci'))
    birthday = Column(Date)
    gender = Column(String(10, collation='utf8_unicode_ci'))
    bookdt = Column(Date)
    depdt = Column(Date)
    carrier = Column(String(10, collation='utf8_unicode_ci'))
    fltno = Column(String(10, collation='utf8_unicode_ci'))
    depstn = Column(String(10, collation='utf8_unicode_ci'))
    arrstn = Column(String(10, collation='utf8_unicode_ci'))
    cbncls = Column(String(10, collation='utf8_unicode_ci'))
    rbd = Column(String(10, collation='utf8_unicode_ci'))

    def __init__(self, data_list):
        self.customer_id = data_list[0]
        self.card_type = data_list[1]
        self.birthday = data_list[2]
        self.gender = data_list[3]
        self.bookdt = data_list[4]
        self.depdt = data_list[5]
        self.carrier = data_list[6]
        self.fltno = data_list[7]
        self.depstn = data_list[8]
        self.arrstn = data_list[9]
        self.cbncls = data_list[10]
        self.rbd = data_list[11]


class TakeFlight(Base):
    __tablename__ = 'take_flight'
    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(String(30, collation='utf8_unicode_ci'))
    carrier = Column(String(10, collation='utf8_unicode_ci'))
    fltno = Column(String(20, collation='utf8_unicode_ci'))
    depdate = Column(Date)
    depstn = Column(String(10, collation='utf8_unicode_ci'))
    arrstn = Column(String(10, collation='utf8_unicode_ci'))
    arcd = Column(String(10, collation='utf8_unicode_ci'))
    tkno = Column(String(50, collation='utf8_unicode_ci'))
    cpno = Column(String(10, collation='utf8_unicode_ci'))
    usdnet = Column(Float)
    flcl = Column(String(10, collation='utf8_unicode_ci'))
    facl = Column(String(10, collation='utf8_unicode_ci'))
    bkcls = Column(String(10, collation='utf8_unicode_ci'))
    turc = Column(String(50, collation='utf8_unicode_ci'))
    turc2 = Column(String(50, collation='utf8_unicode_ci'))
    usdyq = Column(Float)
    mileage = Column(Integer)

    def __init__(self, data_list):
        self.customer_id = data_list[0]
        self.carrier = data_list[1]
        self.fltno = data_list[2]
        self.depdate = data_list[3]
        self.depstn = data_list[4]
        self.arrstn = data_list[5]
        self.arcd = data_list[6]
        self.tkno = data_list[7]
        self.cpno = data_list[8]
        self.usdnet = data_list[9]
        self.flcl = data_list[10]
        self.facl = data_list[11]
        self.bkcls = data_list[12]
        self.turc = data_list[13]
        self.turc2 = data_list[14]
        self.usdyq = data_list[15]
        self.mileage = data_list[16]


class DutyFree(Base):
    __tablename__ = 'duty_free'
    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(String(30, collation='utf8_unicode_ci'))
    po_no = Column(String(10, collation='utf8_unicode_ci'))
    depdate = Column(Date)
    fltno = Column(String(20, collation='utf8_unicode_ci'))
    depstn = Column(String(10, collation='utf8_unicode_ci'))
    arrstn = Column(String(10, collation='utf8_unicode_ci'))
    gender = Column(String(10, collation='utf8_unicode_ci'))
    birthday = Column(Date)
    rbd = Column(String(10, collation='utf8_unicode_ci'))
    cbncls = Column(String(10, collation='utf8_unicode_ci'))
    proc_flg = Column(String(10, collation='utf8_unicode_ci'))
    new_date = Column(DateTime)
    redeem_flg = Column(String(10, collation='utf8_unicode_ci'))
    card_type = Column(String(10, collation='utf8_unicode_ci'))
    redeen_amt = Column(Integer)
    adminbooking_flg = Column(String(10, collation='utf8_unicode_ci'))
    platform = Column(String(20, collation='utf8_unicode_ci'))
    pay_amt_usd = Column(Integer)
    pay_net_usd = Column(Integer)

    def __init__(self, data_list):
        self.customer_id = data_list[0]
        self.po_no = data_list[1]
        self.depdate = data_list[2]
        self.fltno = data_list[3]
        self.depstn = data_list[4]
        self.arrstn = data_list[5]
        self.gender = data_list[6]
        self.birthday = data_list[7]
        self.rbd = data_list[8]
        self.cbncls = data_list[9]
        self.proc_flg = data_list[10]
        self.new_date = string_to_datetime(data_list[11])
        self.redeem_flg = data_list[12]
        self.card_type = data_list[13]
        self.redeen_amt = data_list[14]
        self.adminbooking_flg = data_list[15]
        self.platform = data_list[16]
        self.pay_amt_usd = data_list[17]
        self.pay_net_usd = data_list[18]


def string_to_datetime(text):
    print text
    data = text.split(" ")
    yymmdd = data[0].split("-")
    hhmmss = data[1].split(":")
    print yymmdd
    print hhmmss
    return datetime(int(yymmdd[2]), int(yymmdd[0]), int(yymmdd[1]), int(hhmmss[0]), int(hhmmss[1]), int(hhmmss[2]))

if __name__ == "__main__":
    target_database = 'mysql+pymysql://%s:%s@%s:%s/%s' % (DB_USERNAME, DB_PASSWORD, DB_HOSTNAME,
                                                          DB_PORT, DB_DATABASE)

    engine = create_engine(target_database, echo=True)

    Base.metadata.create_all(engine)

