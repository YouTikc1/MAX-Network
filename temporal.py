"""Source-grounded dates and conservative freshness checks, independent of the model."""
import re
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlsplit

MONTHS={name:i for i,name in enumerate(('января','февраля','марта','апреля','мая','июня','июля','августа','сентября','октября','ноября','декабря'),1)}
MONTH_PATTERN='|'.join(MONTHS)
NAMED_DATE=re.compile(r'(?<!\d)(\d{1,2})\s+('+MONTH_PATTERN+r')(?:\s+((?:19|20)\d{2})(?:\s*г(?:ода|\.)?)?)?',re.I)
NUMERIC_DATE=re.compile(r'(?<!\d)(\d{1,2})[./](\d{1,2})[./]((?:19|20)\d{2})(?!\d)')
ISO_DATE=re.compile(r'(?<!\d)((?:19|20)\d{2})-(\d{2})-(\d{2})(?!\d)')
DATELINE=re.compile(r'(?:^|(?<=[.!?])\s+)[А-ЯЁ][а-яё-]+,\s*\d{1,2}\s+(?:'+MONTH_PATTERN+r')\s+(?:19|20)\d{2}\b',re.M|re.I)


def dates_in(text):
    result=set()
    for match in NAMED_DATE.finditer(text):
        day,month,year=int(match[1]),MONTHS[match[2].lower()],int(match[3]) if match[3] else None
        try:date(year or 2000,month,day)
        except ValueError:continue
        result.add((year,month,day))
    for regex,iso in ((NUMERIC_DATE,False),(ISO_DATE,True)):
        for match in regex.finditer(text):
            year,month,day=map(int,match.groups() if iso else (match[3],match[2],match[1]))
            try:date(year,month,day)
            except ValueError:continue
            result.add((year,month,day))
    return result


def copy_date_problem(text,title,summary):
    if DATELINE.search(text):
        return 'В тексте появилась служебная строка города и даты. Убери её, не подставляй другую дату.'
    source=title+'\n'+summary[:12000]
    source_years=set(re.findall(r'\b(?:19|20)\d{2}\b',source))
    # Do not mistake monetary amounts such as 2 000 / 2000 million for years.
    copy_years={str(year) for year,_,_ in dates_in(text) if year}
    copy_years.update(re.findall(r'\b((?:19|20)\d{2})\s*г(?:од(?:а|у|ом|ы|ов|ах)?|\.)(?!\w)',text,re.I))
    copy_years.update(re.findall(r'\b(?:в|за|к|с|по|на)\s+((?:19|20)\d{2})\b(?!\s*(?:руб|долл|евро|млн|млрд|процент|%))',text,re.I))
    if copy_years-source_years:
        return 'В тексте появился год, которого нет в RSS-источнике. Убери неподтверждённую дату.'
    supported=dates_in(source)
    for year,month,day in dates_in(text):
        if not any((m,d)==(month,day) and (year is None or year==y) for y,m,d in supported):
            return 'Дата в тексте не подтверждена RSS-источником. Не придумывай и не исправляй её на текущую.'
    return ''


def source_date_problem(title,summary,url,published_at,now):
    if not published_at:return 'Нет даты первоначальной публикации RSS — свежесть новости не установлена'
    try:
        published=datetime.fromisoformat(published_at.replace('Z','+00:00'))
        if published.tzinfo is None:published=published.replace(tzinfo=timezone.utc)
    except (ValueError,TypeError):return 'Некорректная дата первоначальной публикации RSS'
    if published>now+timedelta(minutes=15):return 'Дата RSS находится в будущем — не отправляем'
    if now-published>timedelta(hours=48):return 'Новость старше 48 часов — не отправляем'
    # A fresh updated timestamp must not make a dated archive URL into breaking news.
    path=urlsplit(url or '').path
    url_dates=[]
    for pattern in (r'/((?:19|20)\d{2})/(\d{1,2})/(\d{1,2})(?=/|$|[-_])',
                    r'/((?:19|20)\d{2})(\d{2})(\d{2})(?=[/_.-]|$)',
                    r'/((?:19|20)\d{2})-(\d{2})-(\d{2})(?=[/_.-]|$)'):
        for match in re.finditer(pattern,path):
            try:url_dates.append(date(*map(int,match.groups())))
            except ValueError:pass
    if any(value<now.date()-timedelta(days=2) for value in url_dates):
        return 'В URL указан архивный материал; свежая дата RSS не подтверждает новое событие'
    # A newswire dateline is evidence of the story's age, not a historical number
    # in an otherwise fresh quarterly report or comparison.
    for match in DATELINE.finditer(title+'\n'+summary[:12000]):
        for year,month,day in dates_in(match.group()):
            if year and date(year,month,day)<now.date()-timedelta(days=2):
                return 'В источнике указана старая дата сообщения — архив не публикуем как свежую новость'
    return ''
