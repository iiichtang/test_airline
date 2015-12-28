from sqlalchemy import create_engine
from config import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Time, DateTime, Float, Boolean, TIMESTAMP
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy import ForeignKey
import decimal

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

    def __init__(self, id, card_type, birthday, gender, join_date, last_time, times_2013, times_2014, times_2015,
                 revenue_2013, revenue_2014, revenue_2015, award_2013, award_2014, award_2015, award_mileage_2013,
                 award_mileage_2014, award_mileage_2015, tour_code_2013, tour_code_2014, tour_code_2015, duty_free_2013,
                 duty_free_2014, duty_free_2015):
        self.id = id
        self.card_type = card_type
        self.birthday = birthday
        self.gender = gender
        self.join_date = join_date
        self.times_2013 = times_2013
        self.times_2014 = times_2014
        self.times_2015 = times_2015
        self.revenue_2013 = revenue_2013
        self.revenue_2014 = revenue_2014
        self.revenue_2015 = revenue_2015
        self.award_2013 = award_2013
        self.award_2014 = award_2014
        self.award_2015 = award_2015
        self.award_mileage_2013 = award_mileage_2013
        self.award_mileage_2014 = award_mileage_2014
        self.award_mileage_2015 = award_mileage_2015
        self.tour_code_2013 = tour_code_2013
        self.tour_code_2014 = tour_code_2014
        self.tour_code_2015 = tour_code_2015
        self.duty_free_2013 = duty_free_2013
        self.duty_free_2014 = duty_free_2014
        self.duty_free_2015 = duty_free_2015


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

    def __init__(self, customer_id, card_type, birthday, gender, bookdt, depdt, carrier,
                 fltno, depstn, arrstn, cbncls, rbd):
        self.customer_id = customer_id
        self.card_type = card_type
        self.birthday = birthday
        self.gender = gender
        self.bookdt = bookdt
        self.depdt = depdt
        self.carrier = carrier
        self.fltno = fltno
        self.depstn = depstn
        self.arrstn = arrstn
        self.cbncls = cbncls
        self.rbd = rbd


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
    usdnet = Column(DOUBLE)
    flcl = Column(String(10, collation='utf8_unicode_ci'))
    facl = Column(String(10, collation='utf8_unicode_ci'))
    bkcls = Column(String(10, collation='utf8_unicode_ci'))
    turc = Column(String(50, collation='utf8_unicode_ci'))
    turc2 = Column(String(50, collation='utf8_unicode_ci'))
    usdyq = Column(DOUBLE)
    mileage = Column(Integer)

    def __init__(self, customer_id, carrier, fltno, depdate, depstn, arrstn, arcd,
                 tkno, cpno, usdnet, flcl, facl, bkcls, turc, turc2, usdyq, mileage):
        self.customer_id = customer_id
        self.carrier = carrier
        self.fltno = fltno
        self.depdate = depdate
        self.depstn = depstn
        self.arrstn = arrstn
        self.arcd = arcd
        self.tkno = tkno
        self.cpno = cpno
        self.usdnet = usdnet
        self.flcl = flcl
        self.facl = facl
        self.bkcls = bkcls
        self.turc = turc
        self.turc2 = turc2
        self.usdyq = usdyq
        self.mileage = mileage


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
    new_date = Column(Date)
    redeem_flg = Column(String(10, collation='utf8_unicode_ci'))
    card_type = Column(String(10, collation='utf8_unicode_ci'))
    redeen_amt = Column(Integer)
    adminbooking_flg = Column(String(10, collation='utf8_unicode_ci'))
    platform = Column(String(20, collation='utf8_unicode_ci'))
    pay_amt_usd = Column(Integer)
    pay_net_usd = Column(Integer)

    def __init__(self, customer_id, po_no, depdate, fltno, depstn, arrstn, gender,
                 birthday, rbd, cbncls, proc_flg, new_date, redeem_flg, card_type,
                 redeen_amt, adminbooking_flg, platform, pay_amt_usd, pay_net_usd):
        self.customer_id = customer_id
        self.po_no = po_no
        self.depdate = depdate
        self.fltno = fltno
        self.depstn = depstn
        self.arrstn = arrstn
        self.gender = gender
        self.birthday = birthday
        self.rbd = rbd
        self.cbncls = cbncls
        self.proc_flg = proc_flg
        self.new_date = new_date
        self.redeem_flg = redeem_flg
        self.card_type = card_type
        self.redeen_amt = redeen_amt
        self.adminbooking_flg = adminbooking_flg
        self.platform = platform
        self.pay_amt_usd = pay_amt_usd
        self.pay_net_usd = pay_net_usd


if __name__ == "__main__":
    target_database = 'mysql+pymysql://%s:%s@%s:%s/%s' % (DB_USERNAME, DB_PASSWORD, DB_HOSTNAME,
                                                          DB_PORT, DB_DATABASE)

    engine = create_engine(target_database, echo=True)

    Base.metadata.create_all(engine)

