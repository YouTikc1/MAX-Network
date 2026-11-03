"""MAX HTML rendering. Preserve full text; split before transport limits, not by truncation."""
import html
from urllib.parse import urlparse

MAX_MESSAGE_UNITS=4000
PRIMARY_CHANNEL_URL='https://max.ru/channel_neuropulse'
PRIMARY_SIGNATURE='NeuroPulse | Нейросети и ИИ'


def valid_channel_url(value):
    try:
        url=urlparse(value)
        return (url.scheme=='https' and url.hostname in ('max.ru','www.max.ru') and bool(url.path.strip('/'))
                and not url.username and not url.password and url.port in (None,443))
    except ValueError:
        return False


def units(value):
    # Conservative for emoji: count UTF-16 units, including HTML markup.
    return len(value.encode('utf-16-le'))//2


def footer(channel_url,label):
    if not channel_url:return ''
    if not valid_channel_url(channel_url):raise ValueError('Укажите корректную публичную ссылку MAX для подписи')
    return '\n\n<a href="'+html.escape(channel_url,quote=True)+'"><b>'+html.escape(label,quote=False)+'</b></a>'


def message_payloads(text,channel_url='',label='',image_url='',image_urls=None,headline=None):
    # Generated copy has a distinct first paragraph for the headline.
    first, separator, body = text.partition('\n\n')
    # Explicit metadata from the editor wins; never turn a body paragraph into a title.
    if headline is None: headline=first if separator and len(first)<=300 and '\n' not in first else ''
    has_headline=bool(headline and separator and first==headline)
    def render(value, offset):
        escaped=html.escape(value,quote=False)
        if has_headline and offset<len(headline):
            end=min(len(value),len(headline)-offset)
            return '<b>'+html.escape(value[:end],quote=False)+'</b>'+html.escape(value[end:],quote=False)
        return escaped
    signature=footer(channel_url,label)
    budget=MAX_MESSAGE_UNITS-units(signature)
    if budget<100:raise ValueError('Подпись канала слишком длинная')
    chunks=[]
    remaining=text
    offset=0
    while remaining:
        low,high=1,len(remaining)
        end=0
        while low<=high:
            mid=(low+high)//2
            if units(render(remaining[:mid],offset))<=budget:
                end=mid;low=mid+1
            else:high=mid-1
        if not end:raise ValueError('Не удалось разбить сообщение для MAX')
        if end<len(remaining):
            paragraph=remaining.rfind('\n\n',0,end)
            space=remaining.rfind(' ',0,end)
            if paragraph>=end//2:end=paragraph+2
            elif space>=end//2:end=space+1
        chunks.append(render(remaining[:end],offset))
        offset+=end
        remaining=remaining[end:]
    if not chunks:raise ValueError('Текст новости пустой')
    chunks[-1]+=signature
    result=[{'text':chunk,'format':'html'} for chunk in chunks]
    images=list(dict.fromkeys(([image_url] if image_url else [])+list(image_urls or [])))[:10]
    if images:result[0]['attachments']=[{'type':'image','payload':{'url':url}} for url in images]
    assert all(units(part['text'])<=MAX_MESSAGE_UNITS for part in result)
    return result
