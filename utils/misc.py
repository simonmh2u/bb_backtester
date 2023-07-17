from conf.expiry_dates import sorted_expiry_dates
from dateutil.parser import parse


def is_multiple_of_interval(minute, interval):
    if minute % interval == 1:
        return True


def between(z, a, b, inclusive=True):
    """
    True if z is in between a and b
    :param z:
    :param a:
    :param b:
    :param inclusive:
    :return:
    """
    if inclusive:
        return a <= z <= b
    else:
        return a < z < b


def get_nearest_expiry(d):
    for i in sorted_expiry_dates:
        x = parse(i).date()
        if x >= d:
            return x



def get_atm_surrounding_strikes(atm, round_distance=100, length=3):
    upper_strikes = []
    lower_strikes = []
    strike = atm
    for i in range(length):
        strike = strike + round_distance
        upper_strikes.append(strike)
    strike = atm
    for i in range(length):
        strike = strike - round_distance
        lower_strikes.append(strike)
    return lower_strikes, upper_strikes