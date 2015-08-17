
import datetime
import re

def date_from_str(date_str):
    return datetime.datetime.strptime(date_str, '%Y-%m-%d').date()

class TimePeriod(object):

    def __init__(self, from_date, to_date):
        self.from_date = from_date
        self.to_date = to_date

    @staticmethod
    def from_str(period_str):
        m = re.search(r'(\d+\-\d+\-\d+)\s*\.\.\s*(\d+\-\d+\-\d+)', period_str)
        if m:
            try:
                from_date = date_from_str(m.group(1))
                to_date = date_from_str(m.group(2))

                if not from_date <= to_date:
                    raise ValueError()

                return TimePeriod(from_date, to_date)
            except ValueError:
                pass

    def __str__(self):
        return '%s .. %s' % (self.from_date, self.to_date)

    def __repr__(self):
        return '%s(from_date=%s, to_date=%s)' % (self.__class__.__name__, self.from_date, self.to_date)
