from sqlalchemy import Column, Integer, String, Numeric, and_
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
from sqlalchemy.ext.declarative import declared_attr

Base = declarative_base(cls=DeferredReflection)

class UniqueMixin(object):
    @classmethod
    def unique_hash(cls, *arg, **kw):
        raise NotImplementedError()

    @classmethod
    def unique_filter(cls, query, *arg, **kw):
        raise NotImplementedError()

    @classmethod
    def as_unique(cls, session, *arg, **kw):
        return _unique(
                    session,
                    cls,
                    cls.unique_hash,
                    cls.unique_filter,
                    cls,
                    arg, kw)

def _unique(session, cls, hashfunc, queryfunc, constructor, arg, kw):
    cache = getattr(session, '_unique_cache', None)
    if cache is None:
        session._unique_cache = cache = {}

    key = (cls, hashfunc(*arg, **kw))
    if key in cache:
        return cache[key]
    else:
        with session.no_autoflush:
            q = session.query(cls)
            q = queryfunc(q, *arg, **kw)
            obj = q.first()
            if not obj:
                obj = constructor(*arg, **kw)
                session.add(obj)
        cache[key] = obj
        return obj
    
class PSD(object):
   
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    id = Column('id', Integer, primary_key=True, autoincrement=True)
    cmdy_code = Column('Commodity_Code', String(7), nullable=False)
    ctry_code = Column('Country_Code', String(2), nullable=False)
    mkg_yr = Column('Market_Year', Integer, nullable=False)
    cal_yr = Column('Calendar_Year', Integer, nullable=False)
    mth = Column('Month', String(2), nullable=False)
    attr_id = Column('Attribute_ID', String(3), nullable=False)
    unit_id = Column('Unit_ID', String(2), nullable=False)
    val = Column('Value', Numeric(10, 2), nullable=False)
    #val = Column('Value', String(10), nullable=False)
        
    @classmethod
    def unique_hash(cls, cmdy_code, ctry_code, mkg_yr, cal_yr, mth, attr_id, unit_id, val):
        return cmdy_code, ctry_code, mkg_yr, cal_yr, mth, attr_id, unit_id, val

    @classmethod
    def unique_filter(cls, query, cmdy_code, ctry_code, mkg_yr, cal_yr, mth, attr_id, unit_id, val):
        return query.filter(and_(PSD.cmdy_code == cmdy_code,
                                 PSD.ctry_code == ctry_code,
                                 PSD.mkg_yr == mkg_yr,
                                 PSD.cal_yr == cal_yr,
                                 PSD.mth == mth,
                                 PSD.attr_id == attr_id,
                                 PSD.unit_id == unit_id,
                                 PSD.val == val))
        
           
class PSD_ODS(PSD, Base, UniqueMixin):

    def __repr__(self):
        return '<psdODS(cmdy_code={}, ctry_code={}>'.format(PSD.cmdy_code,
                                                            PSD.ctry_code,
                                                            PSD.mkg_yr,
                                                            PSD.cal_yr,
                                                            PSD.mth,
                                                            PSD.attr_id,
                                                            PSD.unit_id,
                                                            PSD.val)

Base.prepare(engine)
