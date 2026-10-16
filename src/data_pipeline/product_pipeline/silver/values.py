"""Exact numeric/date/measurement parsing; interpretation needs explicit evidence."""
from decimal import Decimal, InvalidOperation
import re


def decimal(value):
    try:
        if isinstance(value, (bool, float)) or value is None:
            raise InvalidOperation()
        result = Decimal(str(value).replace(',', '').strip())
        if not result.is_finite() or abs(result) >= Decimal('1e16') or result.as_tuple().exponent < -4:
            return None, 'VALUE_OUT_OF_RANGE'
        return format(result.normalize(), 'f'), 'CONFIRMED'
    except (InvalidOperation, ValueError):
        return None, 'PARSE_FAILED'


def money(value, currency):
    amount, status = decimal(value)
    if amount is not None:
        number = Decimal(amount)
        if number == 0: status='INVALID_ZERO'
        elif number < 0: status='INVALID_NEGATIVE'
        elif currency == 'KRW' and number != number.to_integral(): status='INVALID_FRACTION'
    return amount, status


def display_money(text):
    match = re.fullmatch(r'\s*(?:₩\s*)?(-?\d[\d,]*(?:\.\d+)?)\s*(원|KRW)?\s*', str(text or ''), re.I)
    if not match:
        return None, None
    return match[1].replace(',',''), 'KRW' if match[2] or '₩' in text else None


def registered(text):
    match=re.fullmatch(r'\s*(\d{2}|\d{4})[.\-/년]\s*(\d{1,2})(?:[.월])?\s*', str(text or ''))
    if not match or not 1<=int(match[2])<=12:
        return None, None, 'NOT_PROVIDED' if not text else 'SEMANTICS_UNKNOWN'
    if len(match[1])==2: return None,int(match[2]),'AMBIGUOUS'
    return int(match[1]),int(match[2]),'CONFIRMED'


def measurement(value):
    text=str(value).strip()
    match=re.fullmatch(r'(\d+(?:\.\d+)?)(?:\s*[~～–-]\s*(\d+(?:\.\d+)?))?\s*(mm|cm|m)',text,re.I)
    if not match: return None
    scale={'mm':Decimal(1),'cm':Decimal(10),'m':Decimal(1000)}[match[3].lower()]
    low, status=decimal(Decimal(match[1])*scale)
    high, high_status=decimal(Decimal(match[2])*scale) if match[2] else (None,status)
    if status!='CONFIRMED' or high_status!='CONFIRMED' or Decimal(low)<=0 or (high and Decimal(high)<Decimal(low)):
        return dict(value_decimal=None,value_min=None,value_max=None,unit='mm',attribute_status='VALUE_OUT_OF_RANGE')
    return dict(value_decimal=low if high is None else None,value_min=low if high else None,
                value_max=high,unit='mm',attribute_status='CONFIRMED' if high is None else 'RANGE')


def dimensions(value):
    match=re.fullmatch(r'\s*(\d+(?:\.\d+)?)\s*[xX×]\s*(\d+(?:\.\d+)?)\s*[xX×]\s*(\d+(?:\.\d+)?)\s*(mm|cm|m)\s*',str(value))
    if not match:return None
    parsed=[measurement(v+match[4]) for v in match.group(1,2,3)]
    return [p['value_decimal'] for p in parsed] if all(p and p['value_decimal'] for p in parsed) else None
