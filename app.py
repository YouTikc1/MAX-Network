from __future__ import annotations
import os, sqlite3, hashlib, secrets, asyncio, re, html
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import calendar
import json
from topics import PROFILES, CHANNEL_URLS, brief_for, matches_topic
from publication import selected_images, message_payloads, valid_channel_url, PRIMARY_CHANNEL_URL, PRIMARY_SIGNATURE
from difflib import SequenceMatcher
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse, urljoin, urlsplit, urlunsplit, parse_qsl, urlencode
from typing import Optional
import feedparser, httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from editorial import review, Verdict, EditorialUnavailable, text_hash, validate_copy, DEFAULT_EDITORIAL_BRIEF, LEGACY_BRIEFS, NEWS_REVISION, clean_post
from apscheduler.schedulers.asyncio import AsyncIOScheduler

load_dotenv(Path(__file__).with_name(".env"))
DB = Path(os.getenv('NETWORK_DB_PATH', str(Path(__file__).with_name('network.db'))))
API = os.getenv('MAX_API_BASE', 'https://platform-api.max.ru').rstrip('/')
TOKEN = os.getenv('MAX_TOKEN', '')
PASSWORD = os.getenv('ADMIN_PASSWORD', '')
COLLECT_STATE = {'running': False, 'phase': 'Ожидание', 'found': 0, 'duplicates': 0, 'added': 0, 'errors': [], 'finished': None}
COLLECT_LOCK = asyncio.Lock()
COLLECT_TASK = None
BACKGROUND_ENABLED=os.getenv('NETWORK_BACKGROUND','1')!='0'
LAST_REFILL_AT=None
QUEUE_TARGET=10
FEED_POLL_SECONDS=30
NEWS_FEEDS=('https://habr.com/ru/rss/news/?fl=ru','https://www.ixbt.com/export/news.rss','https://3dnews.ru/news/rss')
ONE_TASKS={}
ONE_JOBS={}
REVIEW_TASK = None
REVIEW_STATE = {"running":False,"done":0,"total":0,"error":None}
COLLECT_STATE.update(filtered=0, sources_done=0, sources_total=0)

def db():
    con = sqlite3.connect(DB, timeout=15)
    con.row_factory = sqlite3.Row
    return con

def init():
    with db() as con:
        con.executescript('''CREATE TABLE IF NOT EXISTS channels (id INTEGER PRIMARY KEY, name TEXT NOT NULL, chat_id TEXT, enabled INTEGER DEFAULT 0, autopublish INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS sources (id INTEGER PRIMARY KEY, channel_id INTEGER NOT NULL, url TEXT NOT NULL, UNIQUE(channel_id,url));
        CREATE TABLE IF NOT EXISTS posts (id INTEGER PRIMARY KEY, channel_id INTEGER NOT NULL, title TEXT, text TEXT NOT NULL, url TEXT, fingerprint TEXT UNIQUE, status TEXT NOT NULL DEFAULT 'draft', created_at TEXT DEFAULT CURRENT_TIMESTAMP, published_at TEXT, error TEXT, remote_id TEXT);''')
        channel_columns={r[1] for r in con.execute('PRAGMA table_info(channels)')}
        for column in ('keywords', 'exclude_keywords'):
            if column not in channel_columns:
                con.execute(f"ALTER TABLE channels ADD COLUMN {column} TEXT NOT NULL DEFAULT ''")
        columns={r[1] for r in con.execute('PRAGMA table_info(posts)')}
        if 'image_url' not in columns: con.execute("ALTER TABLE posts ADD COLUMN image_url TEXT DEFAULT ''")
        if 'image_approved' not in columns: con.execute('ALTER TABLE posts ADD COLUMN image_approved INTEGER DEFAULT 0')
        new_automation = 'verified_chat_id' not in channel_columns
        new_signature = 'channel_url' not in channel_columns
        channel_fields = {
            'topic':"TEXT NOT NULL DEFAULT 'ai'",
            'channel_url':"TEXT NOT NULL DEFAULT ''",
            'signature_text':"TEXT NOT NULL DEFAULT ''",
            'editorial_brief': "TEXT NOT NULL DEFAULT '"+DEFAULT_EDITORIAL_BRIEF+"'",
            'min_score': 'INTEGER NOT NULL DEFAULT 75',
            'interval_minutes': 'INTEGER NOT NULL DEFAULT 60',
            'daily_limit': 'INTEGER NOT NULL DEFAULT 6',
            'start_hour': 'INTEGER NOT NULL DEFAULT 9',
            'end_hour': 'INTEGER NOT NULL DEFAULT 22',
            'timezone': "TEXT NOT NULL DEFAULT 'Europe/Moscow'",
            'verified_chat_id': "TEXT NOT NULL DEFAULT ''",
            'next_publish_at': 'TEXT', 'automation_error': 'TEXT',
        }
        for name, definition in channel_fields.items():
            if name not in channel_columns: con.execute(f'ALTER TABLE channels ADD COLUMN {name} {definition}')
        post_fields = {
            'headline':"TEXT NOT NULL DEFAULT ''",
            'image_urls':"TEXT NOT NULL DEFAULT '[]'",
            'media_mode':"TEXT NOT NULL DEFAULT 'single'",
            'ai_status': "TEXT NOT NULL DEFAULT 'pending'", 'ai_score': 'INTEGER',
            'ai_reason': "TEXT NOT NULL DEFAULT 'Ещё не проверено редактором'",
            'ai_text_hash': "TEXT NOT NULL DEFAULT ''", 'source_summary': "TEXT NOT NULL DEFAULT ''",
            'source_published_at': 'TEXT', 'reviewed_at': 'TEXT', 'published_chat_id': 'TEXT',
            'publish_mode': 'TEXT', 'editorial_revision':'INTEGER NOT NULL DEFAULT 0',
            'news_event':"TEXT NOT NULL DEFAULT ''", 'news_evidence':"TEXT NOT NULL DEFAULT ''",
            'user_edited':'INTEGER NOT NULL DEFAULT 0',
            'remote_ids':"TEXT NOT NULL DEFAULT '[]'",
            'review_attempts':'INTEGER NOT NULL DEFAULT 0', 'review_after':'TEXT', 'review_kind':'TEXT',
        }
        for name, definition in post_fields.items():
            if name not in columns: con.execute(f'ALTER TABLE posts ADD COLUMN {name} {definition}')
        con.execute('CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, message TEXT NOT NULL, channel_id INTEGER, post_id INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP)')
        con.execute("UPDATE posts SET ai_status='pending',ai_score=NULL,ai_text_hash='',ai_reason='Обновлена проверка новостей — редактор проверит автоматически' WHERE status='draft' AND editorial_revision<?",(NEWS_REVISION,))
        con.execute("CREATE TABLE IF NOT EXISTS post_deliveries (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, part_index INTEGER NOT NULL, payload_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', message_id TEXT, error TEXT, UNIQUE(post_id,part_index))")
        con.execute("UPDATE post_deliveries SET status='uncertain',error='Отправка прервана — проверьте канал MAX' WHERE status='sending'")
        if new_signature: con.execute("UPDATE channels SET channel_url=?,signature_text=? WHERE id=1 OR lower(name) LIKE 'neuropulse%'",(PRIMARY_CHANNEL_URL,PRIMARY_SIGNATURE))
        source_columns={r[1] for r in con.execute('PRAGMA table_info(sources)')}
        for name in ('last_checked_at','last_error'):
            if name not in source_columns: con.execute(f'ALTER TABLE sources ADD COLUMN {name} TEXT')
        # Correct only our old stock briefs. User-written editorial policies remain untouched.
        for old_brief in LEGACY_BRIEFS:
            ids=[r['id'] for r in con.execute('SELECT id FROM channels WHERE editorial_brief=?',(old_brief,))]
            for cid in ids:
                con.execute('UPDATE channels SET editorial_brief=? WHERE id=?',(DEFAULT_EDITORIAL_BRIEF,cid))
                con.execute("UPDATE posts SET ai_status='pending',ai_score=NULL,ai_text_hash='',reviewed_at=NULL,ai_reason='Критерии редактора обновлены — нужна повторная оценка' WHERE channel_id=? AND status='draft'",(cid,))
        # Never inherit an old unchecked autopublish flag during upgrade.
        if new_automation: con.execute('UPDATE channels SET autopublish=0')
        con.execute("UPDATE channels SET autopublish=0,automation_error='Прерванная отправка. Сначала проверьте канал MAX.',next_publish_at=NULL WHERE id IN (SELECT channel_id FROM posts WHERE status='publishing')")
        con.execute("UPDATE posts SET status='uncertain',error='Приложение завершилось во время отправки. Проверьте канал MAX.' WHERE status='publishing'")
        existing=con.execute('SELECT COUNT(*) FROM channels').fetchone()[0]
        if not existing:
            for cid, name in [(1,'Нейросети'),(2,'Технологии')]:
                con.execute('INSERT OR IGNORE INTO channels(id,name,chat_id) VALUES (?,?,?)',(cid,name,os.getenv(f'MAX_CHAT_ID_{cid}','')))
            con.execute("UPDATE channels SET channel_url=?,signature_text=? WHERE id=1",(PRIMARY_CHANNEL_URL,PRIMARY_SIGNATURE))
        # One-time additive presets; preserve existing channel settings and deleted presets.
        con.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)')
        if not con.execute("SELECT 1 FROM settings WHERE key='pulse_profiles_v1'").fetchone():
            for topic,(name,_,feeds) in PROFILES.items():
                if topic=='ai': continue
                current=con.execute('SELECT * FROM channels WHERE lower(name) LIKE ?',(name.split(' | ')[0].lower()+'%',)).fetchone()
                if not current and topic=='tech':
                    current=con.execute("SELECT * FROM channels WHERE name='Технологии' AND chat_id='' AND editorial_brief=?",(DEFAULT_EDITORIAL_BRIEF,)).fetchone()
                if current:
                    if current['name']=='Технологии' or (current['topic']=='ai' and current['editorial_brief']==DEFAULT_EDITORIAL_BRIEF):
                        con.execute('UPDATE channels SET name=?,signature_text=?,topic=?,editorial_brief=? WHERE id=?',(name,name,topic,brief_for(topic),current['id']))
                    continue
                con.execute('INSERT INTO channels(name,signature_text,topic,editorial_brief) VALUES (?,?,?,?)',(name,name,topic,brief_for(topic)))
            con.execute("INSERT INTO settings VALUES ('pulse_profiles_v1','1')")
        if not con.execute("SELECT 1 FROM settings WHERE key='pulse_links_v1'").fetchone():
            for topic,(name,_,_) in PROFILES.items():
                con.execute("UPDATE channels SET channel_url=?,signature_text=? WHERE (lower(name) LIKE ? OR (id=1 AND name='Нейросети')) AND (channel_url='' OR channel_url=?)",(CHANNEL_URLS[topic],name,name.split(' | ')[0].lower()+'%',PRIMARY_CHANNEL_URL))
            con.execute("INSERT INTO settings VALUES ('pulse_links_v1','1')")


def auth(authorization: Optional[str] = Header(None)):
    if not PASSWORD or not authorization or not authorization.startswith('Basic '):
        raise HTTPException(401,'Set ADMIN_PASSWORD and sign in',headers={'WWW-Authenticate':'Basic'})
    import base64
    try:
        username, password = base64.b64decode(authorization[6:]).decode().split(':',1)
    except Exception:
        raise HTTPException(401,'Invalid credentials',headers={'WWW-Authenticate':'Basic'})
    if username != 'admin' or not secrets.compare_digest(password,PASSWORD):
        raise HTTPException(401,'Invalid credentials',headers={'WWW-Authenticate':'Basic'})

class SourceIn(BaseModel):
    channel_id: int
    url: str
class ChannelIn(BaseModel):
    name: str
    topic: str = 'ai'
    chat_id: str = ''
    channel_url: str = Field(default='',max_length=2048)
    signature_text: str = Field(default='',max_length=160)
    enabled: bool = False
    autopublish: bool = False
    keywords: str = ''
    exclude_keywords: str = ''
    editorial_brief: str = DEFAULT_EDITORIAL_BRIEF
    min_score: int = Field(default=75, ge=50, le=100)
    interval_minutes: int = Field(default=60, ge=15, le=1440)
    daily_limit: int = Field(default=6, ge=1, le=24)
    start_hour: int = Field(default=9, ge=0, le=23)
    end_hour: int = Field(default=22, ge=1, le=24)
    timezone: str = 'Europe/Moscow'


def utcnow():
    return datetime.now(timezone.utc)


def event(kind, message, channel_id=None, post_id=None):
    with db() as con:
        con.execute('INSERT INTO events(kind,message,channel_id,post_id) VALUES (?,?,?,?)', (kind,message,channel_id,post_id))


def ai_result(verdict, url, min_score):
    approved=verdict.publishable and verdict.score>=min_score
    if verdict.publishable and verdict.score<min_score:
        verdict.reason=f'Оценка {verdict.score} ниже порога {min_score}. '+verdict.reason
    text=verdict.text.strip() if verdict.publishable else ''
    text=clean_post(text)
    return ('approved' if approved else 'rejected', text, verdict.reason, verdict.score)

def safe_image_url(url):
    """Only public HTTPS image links; MAX downloads image itself."""
    import ipaddress
    try:
        u=urlparse(str(url or '').strip())
        port=u.port
    except ValueError:
        return ''
    if u.scheme!='https' or not u.hostname or u.username or u.password or port not in (None,443): return ''
    if u.hostname.lower() in ('localhost',) or u.hostname.lower().endswith(('.local','.internal')): return ''
    try:
        if not ipaddress.ip_address(u.hostname).is_global: return ''
    except ValueError: pass
    return str(url).strip()[:2048]

def feed_images(entry):
    full=[]
    thumbnails=[]
    for item in entry.get('media_content',[]):
        if isinstance(item,dict) and (not item.get('type') or str(item['type']).startswith('image/')):
            full.append(item.get('url',''))
    for item in entry.get('enclosures',[]):
        if str(item.get('type','')).startswith('image/'): full.append(item.get('href',''))
    for item in entry.get('media_thumbnail',[]):
        if isinstance(item,dict): thumbnails.append(item.get('url',''))
    for key in ('image','thumbnail'):
        value=entry.get(key)
        url=(value.get('href') or value.get('url')) if isinstance(value,dict) else value
        if isinstance(url,str): (full if key=='image' else thumbnails).append(url)
    embedded=[]
    for block in [entry.get('summary','')]+[x.get('value','') for x in entry.get('content',[]) if isinstance(x,dict)]:
        embedded.extend(html.unescape(m.group(1)) for m in re.finditer(r'<img\b[^>]*?\bsrc=[\"\']([^\"\']+)',block,re.I))
    # Prefer full-size metadata, then article images, then thumbnails. Do not
    # merge parallel representations of the same article illustration.
    for group in (full,embedded,thumbnails):
        urls=[safe_image_url(urljoin(entry.get('link',''),str(url or ''))) for url in group]
        images=selected_images('',[url for url in urls if url],gallery=True)
        if images: return images
    return []

def feed_image(entry):
    return next(iter(feed_images(entry)), '')

def feed_summary(entry):
    blocks=[entry.get('summary','')]+[item.get('value','') for item in entry.get('content',[]) if isinstance(item,dict)]
    cleaned=[]
    for block in blocks:
        value=re.sub(r'<(script|style)\b[^>]*>.*?</\1>', '', str(block or ''), flags=re.S|re.I)
        value=html.unescape(re.sub('<[^>]+>',' ',value))
        value=re.sub(r'\s+',' ',value).strip()
        if value: cleaned.append(value)
    return max(cleaned,key=len)[:12000] if cleaned else ''


def canonical_url(url):
    parts=urlsplit(url)
    query=[(k,v) for k,v in parse_qsl(parts.query, keep_blank_values=True)
           if not k.lower().startswith('utm_') and k.lower() not in ('fbclid','gclid','yclid')]
    return urlunsplit((parts.scheme.lower(),parts.netloc.lower(),parts.path or '/',urlencode(sorted(query)),''))

def interesting(title, summary, channel):
    text=(title+' '+summary).casefold()
    excluded=[x.strip().casefold() for x in channel['exclude_keywords'].split(',') if x.strip()]
    included=[x.strip().casefold() for x in channel['keywords'].split(',') if x.strip()]
    return not any(x in text for x in excluded) and (not included or any(x in text for x in included))

def validate_channel(item):
    if item.topic not in PROFILES: raise HTTPException(400,'Неизвестная тематика канала')
    if not item.name.strip() or len(item.name)>120: raise HTTPException(400,'Название: 1–120 символов')
    if item.chat_id and not re.fullmatch(r'-?\d+',item.chat_id.strip()): raise HTTPException(400,'Chat ID должен быть числом')
    if item.channel_url and not valid_channel_url(item.channel_url.strip()): raise HTTPException(400,'Подпись должна вести на публичную HTTPS-ссылку max.ru')
    if item.start_hour>=item.end_hour: raise HTTPException(400,'Начало окна должно быть раньше окончания')
    try: ZoneInfo(item.timezone)
    except Exception: raise HTTPException(400,'Неизвестный часовой пояс')
    if not item.editorial_brief.strip() or len(item.editorial_brief)>2000: raise HTTPException(400,'Задайте тематику канала: 1–2000 символов')

def ensure_news_source(cid):
    with db() as con:
        ch=con.execute('SELECT * FROM channels WHERE id=?',(cid,)).fetchone()
        if ch:
            con.executemany('INSERT OR IGNORE INTO sources(channel_id,url) VALUES (?,?)',[(cid,url) for url in PROFILES[ch['topic']][2]])


async def collect(channel_id, stats=None, stop_after_ready=False,review_after=True,auto_send=True):
    with db() as con:
        sources=con.execute('SELECT * FROM sources WHERE channel_id=? ORDER BY id',(channel_id,)).fetchall()
        channel=con.execute('SELECT * FROM channels WHERE id=?',(channel_id,)).fetchone()
    added=0
    async with httpx.AsyncClient(timeout=25,follow_redirects=True) as client:
        for source in sources:
            url=source['url']
            if stats is not None: stats['phase']='Читаем RSS: '+url[:90]
            try:
                response=await client.get(url,headers={'User-Agent':'Mozilla/5.0 (compatible; NeuroPulseRSS/2.0)'})
                response.raise_for_status()
                feed=feedparser.parse(response.content)
                if not feed.entries: raise ValueError('Лента не содержит записей')
                for entry in feed.entries[:100]:
                    link=entry.get('link','')
                    if not link.startswith(('http://','https://')): continue
                    link=canonical_url(link)
                    title=html.unescape(re.sub('<[^>]+>','',entry.get('title','')))[:250]
                    summary=feed_summary(entry)
                    parsed=entry.get('published_parsed') or entry.get('updated_parsed')
                    published=datetime.fromtimestamp(calendar.timegm(parsed),timezone.utc).isoformat() if parsed else None
                    fingerprint=hashlib.sha256((str(channel_id)+'|'+link).encode()).hexdigest()
                    if stats is not None: stats['found']+=1
                    with db() as con:
                        old=con.execute('SELECT * FROM posts WHERE channel_id=? AND (url=? OR fingerprint=?)',(channel_id,link,fingerprint)).fetchone()
                        if old:
                            if published: con.execute('UPDATE posts SET source_published_at=COALESCE(source_published_at,?) WHERE id=?',(published,old['id']))
                            if old['status']=='draft' and summary and (not old['source_summary'] or len(summary)>len(old['source_summary'])+200):
                                con.execute("UPDATE posts SET source_summary=?,ai_status='pending',ai_text_hash='',review_after=NULL,review_attempts=0,ai_reason='Дополнен исходный материал — редактор проверит автоматически' WHERE id=?",(summary,old['id']))
                            if stats is not None: stats['duplicates']+=1
                            continue
                        reason=None
                        if not matches_topic(title,summary,channel['topic']): reason='Материал не соответствует тематике канала'
                        elif not interesting(title,summary,channel): reason='Не соответствует фильтрам канала'
                        elif published and utcnow()-datetime.fromisoformat(published)>timedelta(hours=48): reason='Новость старше 48 часов'
                        state='rejected' if reason else 'pending'
                        cur=con.execute('INSERT OR IGNORE INTO posts(channel_id,title,text,url,fingerprint,image_url,image_urls,source_summary,source_published_at,ai_status,ai_reason,ai_score,editorial_revision) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                            (channel_id,title,title+'\n\n'+summary[:900],link,fingerprint,feed_image(entry),json.dumps(feed_images(entry)),summary,published,state,reason or 'Ожидает проверки редактором',0 if reason else None,NEWS_REVISION))
                        added+=cur.rowcount
                        if stats is not None:
                            stats['added']+=cur.rowcount
                            if reason: stats['filtered']+=cur.rowcount
                with db() as con: con.execute('UPDATE sources SET last_checked_at=?,last_error=NULL WHERE id=?',(utcnow().isoformat(),source['id']))
            except Exception as exc:
                message=url+': '+str(exc)[:160]
                with db() as con: con.execute('UPDATE sources SET last_checked_at=?,last_error=? WHERE id=?',(utcnow().isoformat(),message,source['id']))
                if stats is not None: stats['errors'].append(message)
            finally:
                if stats is not None: stats['sources_done']+=1
    if review_after and not REVIEW_STATE['running']:
        REVIEW_STATE['running']=True
        await review_pending(channel_id=channel_id,target=1 if stop_after_ready else QUEUE_TARGET,auto_send=auto_send)
    return added

PUBLISH_LOCK = asyncio.Lock()

def automation_ready(channel, now):
    if not channel['enabled'] or not channel['autopublish'] or not TOKEN: return False
    if not channel['chat_id'] or channel['verified_chat_id']!=channel['chat_id']: return False
    if channel['automation_error']: return False
    with db() as con:
        if con.execute("SELECT 1 FROM posts WHERE channel_id=? AND status='uncertain'",(channel['id'],)).fetchone(): return False
    return True


def auto_candidate(row,channel,now):
    if not matches_topic(row['title'] or '',row['source_summary'] or '',channel['topic']): return False
    if row['editorial_revision']!=NEWS_REVISION or not row['news_event'] or not row['news_evidence']: return False
    if not row['source_published_at']: return False
    if datetime.fromisoformat(row['source_published_at'])>now+timedelta(minutes=15): return False
    if row['ai_status']!='approved' or row['ai_score'] is None or row['ai_score']<channel['min_score']: return False
    if not interesting(row['title'] or '',row['source_summary'] or row['text'],channel): return False
    if row['ai_text_hash']!=text_hash(row['text']) or not row['reviewed_at'] or row['error']: return False
    if not validate_copy(clean_post(row['text'])): return False
    if now-datetime.fromisoformat(row['reviewed_at'])>timedelta(hours=24): return False
    created=datetime.fromisoformat(row['created_at'].replace(' ','T')).replace(tzinfo=timezone.utc)
    if now-created>timedelta(hours=48): return False
    if row['source_published_at'] and now-datetime.fromisoformat(row['source_published_at'])>timedelta(hours=48): return False
    with db() as con:
        previous=con.execute("SELECT title,url FROM posts WHERE channel_id=? AND status IN ('published','uncertain')",(channel['id'],)).fetchall()
    title=re.sub(r'\W+',' ',row['title'] or '').strip().casefold()
    for other in previous:
        other_title=re.sub(r'\W+',' ',other['title'] or '').strip().casefold()
        if row['url'] and other['url'] and canonical_url(row['url'])==canonical_url(other['url']): return False
        if same_event(title,other_title): return False
    return True


async def publish(post_id, automatic=False):
    async with PUBLISH_LOCK:
        return await _publish(post_id,automatic)


async def _publish(post_id, automatic=False):
    now=utcnow()
    with db() as con:
        row=con.execute('SELECT * FROM posts WHERE id=?',(post_id,)).fetchone()
        if not row: raise ValueError('Пост не найден')
        channel=con.execute('SELECT * FROM channels WHERE id=?',(row['channel_id'],)).fetchone()
    if row['status']!='draft': raise ValueError('Пост уже отправлен или требует сверки с MAX')
    if not TOKEN or not channel['chat_id']: raise ValueError('Не настроены MAX_TOKEN или Chat ID')
    if automatic and not channel['enabled']: raise ValueError('Автопостинг канала отключён')
    if row['text'].startswith('⚠️ Требуется обработка ИИ'): raise ValueError('Сначала обработайте черновик через Ollama')
    if automatic and (not automation_ready(channel,now) or not auto_candidate(row,channel,now)):
        raise ValueError('Пост или канал не прошли проверку автопубликации')
    if not matches_topic(row['title'] or '',row['source_summary'] or '',channel['topic']): raise ValueError('Материал не соответствует тематике канала — отправка заблокирована')
    if not validate_copy(clean_post(row['text'])):
        raise ValueError('Текст содержит инструкции, пересказ статьи или некорректный формат. Редактор подготовит новую версию.')
    if row['editorial_revision']!=NEWS_REVISION or row['ai_status']!='approved' or row['ai_text_hash']!=text_hash(row['text']) or not row['news_event'] or not row['news_evidence']:
        raise ValueError('Редактор ещё проверяет новость. Непроверенный текст отправлен не будет.')
    payloads=message_payloads(clean_post(row['text']),channel['channel_url'],channel['signature_text'] or channel['name'],row['image_url'] or '',json.loads(row['image_urls']) if row['image_url'] else [],headline=row['headline'],gallery=row['media_mode']=='gallery')
    with db() as con:
        # Durable claim prevents retries after a restart or another worker.
        cur=con.execute("UPDATE posts SET status='publishing',remote_ids='[]',remote_id=NULL,publish_mode=?,published_chat_id=? WHERE id=? AND status='draft'",
                        ('auto' if automatic else 'manual',channel['chat_id'],post_id))
        if not cur.rowcount: raise ValueError('Отправка уже началась')
        con.execute('DELETE FROM post_deliveries WHERE post_id=?',(post_id,))
        con.executemany('INSERT INTO post_deliveries(post_id,part_index,payload_json) VALUES (?,?,?)',[(post_id,i,json.dumps(payload,ensure_ascii=False)) for i,payload in enumerate(payloads)])
    remote_ids=[]
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            for part_index,payload in enumerate(payloads):
                with db() as con:
                    con.execute("UPDATE post_deliveries SET status='sending' WHERE post_id=? AND part_index=?",(post_id,part_index))
                response=await client.post(f'{API}/messages',params={'chat_id':channel['chat_id'],'disable_link_preview':'true'},headers={'Authorization':TOKEN},json=payload)
                response.raise_for_status()
                remote_id=str(response.json().get('message',{}).get('body',{}).get('mid',''))
                if not remote_id: raise ValueError('MAX не вернул идентификатор сообщения')
                remote_ids.append(remote_id)
                with db() as con:
                    con.execute("UPDATE post_deliveries SET status='published',message_id=? WHERE post_id=? AND part_index=?",(remote_id,post_id,part_index))
                    con.execute('UPDATE posts SET remote_ids=? WHERE id=?',(json.dumps(remote_ids),post_id))
        remote_id=remote_ids[0]
        with db() as con:
            con.execute("UPDATE posts SET status='published',published_at=?,remote_id=?,error=NULL WHERE id=?",(now.isoformat(),remote_id,post_id))
            con.execute('UPDATE channels SET verified_chat_id=?,next_publish_at=?,automation_error=NULL WHERE id=? AND chat_id=?',
                (channel['chat_id'],None,channel['id'],channel['chat_id']))
        refresh_reserve()
        request_refill()
        event('published','Пост отправлен '+('автоматически' if automatic else 'вручную'),channel['id'],post_id)
        return {'ok':True,'remote_id':remote_id,'remote_ids':remote_ids,'parts':len(remote_ids)}
    except BaseException as exc:
        certain=not remote_ids and isinstance(exc,httpx.HTTPStatusError) and 400<=exc.response.status_code<500
        state='draft' if certain else 'uncertain'
        message=('MAX отклонил запрос' if certain else 'Результат отправки неизвестен. Проверьте канал MAX, затем подтвердите результат в панели.')
        if remote_ids: message=f'Отправлено частей {len(remote_ids)} из {len(payloads)}. Не повторяйте новость: сначала проверьте канал MAX.'
        with db() as con:
            con.execute("UPDATE post_deliveries SET status=?,error=? WHERE post_id=? AND status='sending'",('failed' if certain else 'uncertain',message,post_id))
            con.execute('UPDATE posts SET status=?,error=? WHERE id=?',(state,message,post_id))
            if automatic: con.execute('UPDATE channels SET autopublish=0,automation_error=?,next_publish_at=NULL WHERE id=?',(message,channel['id']))
        event('error',message,channel['id'],post_id)
        raise


async def automation_tick(channel_id=None):
    if PUBLISH_LOCK.locked(): return
    with db() as con:
        query='SELECT * FROM channels WHERE enabled=1 AND autopublish=1'
        channels=con.execute(query+(' AND id=?' if channel_id is not None else ''),[channel_id] if channel_id is not None else []).fetchall()
    for channel in channels:
        if ONE_JOBS.get(channel['id'],{}).get('running'): continue
        if not automation_ready(channel,utcnow()): continue
        with db() as con:
            candidates=con.execute("SELECT * FROM posts WHERE channel_id=? AND status='draft' AND ai_status='approved' ORDER BY source_published_at DESC,ai_score DESC,id DESC",(channel['id'],)).fetchall()
        for post in candidates:
            if not auto_candidate(post,channel,utcnow()): continue
            try: await publish(post['id'],automatic=True)
            except Exception: break  # Durable error + pause; do not retry uncertain sends.


async def run_collection(enabled_only=False, channel_id=None, stop_after_ready=False,review_after=True,auto_send=True):
    global LAST_REFILL_AT
    async with COLLECT_LOCK:
        LAST_REFILL_AT=utcnow()
        COLLECT_STATE.update(running=True, phase='Начинаем сбор', found=0, duplicates=0,
                             added=0, filtered=0, sources_done=0, sources_total=0, errors=[], finished=None)
        try:
            with db() as con:
                query = 'SELECT id FROM channels' + (' WHERE enabled=1' if enabled_only else '')
                ids = [row['id'] for row in con.execute(query)]
                if channel_id is not None: ids=[cid for cid in ids if cid==channel_id]
                for cid in ids: ensure_news_source(cid)
                COLLECT_STATE['sources_total'] = sum(con.execute('SELECT COUNT(*) FROM sources WHERE channel_id=?', (cid,)).fetchone()[0] for cid in ids)
            # Fetch independently of model inference so a slow local model does not
            # prevent the next RSS poll or delay discovery in the other channels.
            semaphore=asyncio.Semaphore(3)
            async def fetch_channel(cid):
                async with semaphore:
                    await collect(cid,COLLECT_STATE,review_after=False)
            await asyncio.gather(*(fetch_channel(cid) for cid in ids))
            if review_after and not REVIEW_STATE['running']:
                REVIEW_STATE['running']=True
                await review_pending(channel_id=channel_id,target=1 if stop_after_ready else QUEUE_TARGET,auto_send=auto_send)
        except Exception as exc:
            COLLECT_STATE['errors'].append(type(exc).__name__ + ': ошибка сбора')
        finally:
            COLLECT_STATE.update(running=False, phase='Готово', finished=datetime.now(timezone.utc).isoformat())

async def tick():
    if not COLLECT_LOCK.locked() and not COLLECT_STATE['running']:
        refresh_reserve()
        COLLECT_STATE['running']=True
        await run_collection(review_after=False)
        queue_review()
        await automation_tick()

@asynccontextmanager
async def lifespan(app):
    init()
    scheduler=AsyncIOScheduler(timezone='UTC')
    if BACKGROUND_ENABLED:
        scheduler.add_job(tick,'interval',seconds=FEED_POLL_SECONDS,max_instances=1,coalesce=True)
        scheduler.add_job(automation_tick,'interval',seconds=5,max_instances=1,coalesce=True)
    scheduler.start()
    global COLLECT_TASK
    if BACKGROUND_ENABLED: COLLECT_TASK=asyncio.create_task(tick())
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)
        for task in (COLLECT_TASK, REVIEW_TASK, *ONE_TASKS.values()):
            if task and not task.done():
                task.cancel()
                try: await task
                except asyncio.CancelledError: pass
app=FastAPI(title='MAX Network',lifespan=lifespan)
app.mount('/static',StaticFiles(directory=Path(__file__).with_name('static')),name='static')

@app.get('/',response_class=HTMLResponse)
def home(_=Depends(auth)):
    return Path(__file__).with_name('templates').joinpath('index.html').read_text(encoding='utf-8')

def max_api_failure(response):
    status=response.status_code
    if status==401: return 'MAX отклонил токен (HTTP 401). Проверьте MAX_TOKEN и перезапустите сервер.'
    if status==403: return 'MAX запретил этот запрос (HTTP 403). Проверьте доступ бота к API.'
    if status==404: return 'Этот метод или чат недоступен в MAX (HTTP 404). Это не означает неверный токен.'
    if status==409: return 'MAX отклонил чтение событий (HTTP 409). Проверьте, не использует ли другой сервис события этого бота.'
    if status==429: return 'Лимит запросов MAX (HTTP 429). Повторите позже.'
    return f'MAX вернул HTTP {status}. Повторите позже.'


@app.get('/api/max/chats',dependencies=[Depends(auth)])
async def max_chats():
    if not TOKEN: raise HTTPException(400,'Сначала настройте MAX_TOKEN в окружении сервера и перезапустите сервер')
    # /chats list is no longer supported. Read a single pending event batch without
    # advancing its marker, changing webhook subscriptions or acknowledging events.
    cache_key='max_discovered_chats:'+hashlib.sha256(TOKEN.encode()).hexdigest()
    with db() as con:
        cached=con.execute('SELECT value FROM settings WHERE key=?',(cache_key,)).fetchone()
        known={row['chat_id'] for row in con.execute("SELECT chat_id FROM channels WHERE chat_id!=''")}
    if cached: known.update(json.loads(cached['value']))
    warnings=[]
    chats=[]
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response=await client.get(API+'/updates',headers={'Authorization':TOKEN},params={'limit':100,'timeout':0})
            if response.is_error:
                if not known: raise HTTPException(502,max_api_failure(response))
                warnings.append(max_api_failure(response))
            else:
                data=response.json()
                if not isinstance(data,dict) or not isinstance(data.get('updates'),list): raise ValueError('Invalid updates')
                for update in data['updates']:
                    if not isinstance(update,dict): continue
                    cid=update.get('chat_id')
                    if cid is None:
                        message=update.get('message')
                        recipient=message.get('recipient') if isinstance(message,dict) else None
                        if isinstance(recipient,dict): cid=recipient.get('chat_id')
                    if isinstance(cid,int) and not isinstance(cid,bool): known.add(str(cid))
            for cid in sorted(known):
                if not re.fullmatch(r'-?\d+',cid): continue
                response=await client.get(API+'/chats/'+cid,headers={'Authorization':TOKEN})
                if response.status_code in (403,404): continue
                if response.is_error: raise HTTPException(502,max_api_failure(response))
                chat=response.json()
                if chat.get('type')!='channel' or chat.get('status')!='active': continue
                if str(chat.get('chat_id'))!=cid: raise ValueError('Invalid chat ID')
                chats.append({'chat_id':cid,'title':chat.get('title') or 'Канал '+cid,'link':chat.get('link') or ''})
    except httpx.TransportError:
        raise HTTPException(502,'Не удалось подключиться к MAX. Проверьте соединение сервера с platform-api.max.ru.')
    except (ValueError,KeyError,TypeError):
        raise HTTPException(502,'MAX вернул неожиданный формат данных. Токен менять не требуется.')
    with db() as con:
        con.execute('INSERT OR REPLACE INTO settings(key,value) VALUES (?,?)',(cache_key,json.dumps([chat['chat_id'] for chat in chats])))
    return {'chats':chats,'warnings':warnings,'hint':'Каналы определяются по доступным событиям бота, а не полному списку. Если нужного канала нет, опубликуйте в нём короткое сообщение вручную в MAX и повторите поиск. Если события получает другой сервис или настроен webhook, возьмите Chat ID из его событий. Бот должен получать события канала.'}

@app.get('/api/channels',dependencies=[Depends(auth)])
def channels():
    with db() as con: return [dict(x) for x in con.execute('SELECT * FROM channels')]
def save_channel(item, cid=None):
    validate_channel(item)
    values=item.model_dump()
    values.update(name=item.name.strip(),chat_id=item.chat_id.strip(),enabled=int(item.enabled),autopublish=int(item.autopublish))
    with db() as con:
        current=con.execute('SELECT * FROM channels WHERE id=?',(cid,)).fetchone() if cid else None
        if cid and not current: raise HTTPException(404,'Канал не найден')
        if current:
            for field in ('channel_url','signature_text','topic'):
                if field not in item.model_fields_set: values[field]=current[field]
        if values['topic']!='ai' and values['editorial_brief']==DEFAULT_EDITORIAL_BRIEF:
            values['editorial_brief']=brief_for(values['topic'])
        values['channel_url']=values['channel_url'].strip()
        values['signature_text']=values['signature_text'].strip()
        if item.autopublish:
            if not item.enabled or not TOKEN or not item.chat_id: raise HTTPException(400,'Включите канал и задайте Chat ID и токен')
            if con.execute("SELECT 1 FROM posts WHERE channel_id=? AND status='uncertain'",(cid,)).fetchone():
                raise HTTPException(400,'Сначала подтвердите результат неизвестной отправки в очереди')
            if not current or current['verified_chat_id']!=item.chat_id.strip():
                raise HTTPException(400,'Сначала отправьте один пост вручную в этот Chat ID. Это проверит права бота.')
        changed=current and (current['topic']!=values['topic'] or current['editorial_brief']!=item.editorial_brief or current['keywords']!=item.keywords or current['exclude_keywords']!=item.exclude_keywords or current['min_score']!=item.min_score)
        if changed:
            con.execute("UPDATE posts SET ai_status='pending',ai_text_hash='',ai_reason='Тематика канала изменена — нужна повторная оценка' WHERE channel_id=? AND status='draft'",(cid,))
        enabled_now=item.autopublish and (not current or not current['autopublish'])
        values['next_publish_at']=None
        values['automation_error']=None
        if current:
            if current['chat_id']!=item.chat_id.strip(): values['verified_chat_id']=''
            con.execute('UPDATE channels SET '+','.join(k+'=?' for k in values)+' WHERE id=?',list(values.values())+[cid])
        else:
            cur=con.execute('INSERT INTO channels('+','.join(values)+') VALUES ('+','.join('?' for _ in values)+')',list(values.values()))
            cid=cur.lastrowid
    if enabled_now: event('automation','Автопостинг включён: отправка сразу после проверки, круглосуточно, без интервала и дневного лимита',cid)
    return {'ok':True,'id':cid}


@app.put('/api/channels/{cid}',dependencies=[Depends(auth)])
async def update_channel(cid:int,item:ChannelIn):
    if PUBLISH_LOCK.locked(): raise HTTPException(409,'Дождитесь завершения публикации')
    return save_channel(item,cid)


@app.post('/api/channels',dependencies=[Depends(auth)])
async def create_channel(item:ChannelIn):
    return save_channel(item)

@app.delete('/api/channels/{cid}',dependencies=[Depends(auth)])
async def delete_channel(cid:int):
    if COLLECT_STATE['running'] or PUBLISH_LOCK.locked(): raise HTTPException(409, 'Дождитесь завершения операции')
    with db() as con:
        if con.execute('SELECT 1 FROM sources WHERE channel_id=?', (cid,)).fetchone() or con.execute('SELECT 1 FROM posts WHERE channel_id=?', (cid,)).fetchone():
            raise HTTPException(409, 'Канал содержит источники или историю публикаций. Отключите его вместо удаления.')
        if not con.execute('DELETE FROM channels WHERE id=?', (cid,)).rowcount: raise HTTPException(404, 'Канал не найден')
    return {'ok':True}

@app.get('/api/sources',dependencies=[Depends(auth)])
def sources():
    with db() as con: return [dict(x) for x in con.execute('SELECT * FROM sources')]
@app.post('/api/sources',dependencies=[Depends(auth)])
async def add_source(item:SourceIn):
    url = item.url.strip()
    if not safe_image_url(url): raise HTTPException(400,'Укажите публичный HTTPS URL источника')
    with db() as con:
        if not con.execute('SELECT 1 FROM channels WHERE id=?',(item.channel_id,)).fetchone(): raise HTTPException(404,'Канал не найден')
        cur=con.execute('INSERT OR IGNORE INTO sources(channel_id,url) VALUES (?,?)',(item.channel_id,url))
        if not cur.rowcount: raise HTTPException(409,'Источник уже добавлен')
    global COLLECT_TASK
    if BACKGROUND_ENABLED and not COLLECT_STATE['running']:
        COLLECT_STATE['running']=True
        COLLECT_TASK=asyncio.create_task(manual_pipeline())
    return {'ok':True}
@app.delete('/api/sources/{sid}',dependencies=[Depends(auth)])
def remove_source(sid:int):
    with db() as con: con.execute('DELETE FROM sources WHERE id=?',(sid,))
    return {'ok':True}
@app.get('/api/posts',dependencies=[Depends(auth)])
def posts():
    ready={p['id'] for c in channels() for p in ready_posts(c['id'])}
    with db() as con: rows=[dict(x) for x in con.execute("SELECT * FROM posts WHERE status!='deleted' ORDER BY id DESC LIMIT 300")]
    for row in rows: row['queue_ready']=row['id'] in ready
    return rows
@app.get('/api/collect/status', dependencies=[Depends(auth)])
def collect_status():
    return dict(COLLECT_STATE)

@app.post('/api/collect', dependencies=[Depends(auth)])
async def collect_now():
    global COLLECT_TASK
    if COLLECT_STATE['running'] or COLLECT_LOCK.locked() or REVIEW_STATE['running']:
        raise HTTPException(409, 'Сбор уже выполняется')
    COLLECT_STATE.update(running=True, phase='Начинаем сбор', errors=[], finished=None)
    COLLECT_TASK = asyncio.create_task(manual_pipeline())
    return {'started':True}
@app.put('/api/posts/{pid}',dependencies=[Depends(auth)])
async def edit_post(pid:int,item:dict):
    if PUBLISH_LOCK.locked(): raise HTTPException(409,'Дождитесь завершения публикации')
    value=clean_post(item.get('text',''))
    if not value: raise HTTPException(400,'Текст не должен быть пустым')
    image_url=str(item.get('image_url','')).strip()
    if image_url and not safe_image_url(image_url): raise HTTPException(400,'Image must be a public HTTPS URL')
    approved=int(bool(item.get('image_approved',False))) if image_url else 0
    with db() as con:
        row=con.execute("SELECT * FROM posts WHERE id=? AND status='draft'",(pid,)).fetchone()
        if not row: raise HTTPException(400,'Редактировать можно только черновики')
        images=item.get('image_urls',json.loads(row['image_urls']) if image_url==row['image_url'] else [])
        if not isinstance(images,list) or len(images)>10 or any(not isinstance(url,str) or not safe_image_url(url) for url in images):
            raise HTTPException(400,'Укажите не более 10 публичных HTTPS URL изображений')
        media_mode=item.get('media_mode',row['media_mode'])
        if media_mode not in ('single','gallery'): raise HTTPException(400,'Неизвестный режим изображений')
        if not image_url: images=[]
        changed=value!=row['text']
        con.execute("UPDATE posts SET text=?,image_url=?,image_urls=?,media_mode=?,image_approved=?,ai_status=?,ai_text_hash=?,ai_reason=?,user_edited=?,error=NULL WHERE id=?",
            (value,image_url,json.dumps(images),media_mode,approved,'pending' if changed else row['ai_status'],'' if changed else row['ai_text_hash'],
             'Текст проверяется редактором автоматически' if changed else row['ai_reason'],int(changed or row['user_edited']),pid))
    if changed: queue_review()
    return {'ok':True}
@app.post('/api/posts/{pid}/publish',dependencies=[Depends(auth)])
async def publish_now(pid:int):
    try:
        if REVIEW_TASK and not REVIEW_TASK.done(): await asyncio.shield(REVIEW_TASK)
        with db() as con: row=con.execute('SELECT * FROM posts WHERE id=?',(pid,)).fetchone()
        if not row: raise HTTPException(404,'Пост не найден')
        if row['status']=='draft' and (row['editorial_revision']!=NEWS_REVISION or row['ai_status']!='approved' or not validate_copy(clean_post(row['text']))):
            await rewrite_post(pid)
        return await publish(pid)
    except HTTPException: raise
    except ValueError as exc: raise HTTPException(400,str(exc))
    except Exception: raise HTTPException(502,'Ошибка MAX API. Проверьте доступ, токен и права бота; перед повтором проверьте канал.')

@app.delete('/api/posts/{pid}',dependencies=[Depends(auth)])
async def delete_post(pid:int):
    async with PUBLISH_LOCK:
        with db() as con:
            # Retain fingerprint to prevent deleted stories from reappearing on the next collection.
            cur=con.execute("UPDATE posts SET status='deleted' WHERE id=? AND status='draft'",(pid,))
            if not cur.rowcount: raise HTTPException(404,'Черновик не найден')
    request_refill()
    return {'ok':True}

@app.get('/api/health',dependencies=[Depends(auth)])
async def health():
    result={'max_token_configured':bool(TOKEN),'ollama':False,'model':os.getenv('OLLAMA_MODEL','qwen2.5:3b'),'autopublish':True}
    try:
        async with httpx.AsyncClient(timeout=5,trust_env=False) as client:
            response=await client.get(os.getenv('OLLAMA_URL','http://127.0.0.1:11434').rstrip('/')+'/api/tags')
            response.raise_for_status()
            result['ollama']=any(m.get('name')==result['model'] for m in response.json().get('models',[]))
    except Exception:
        pass
    return result

@app.post('/api/posts/{pid}/rewrite',dependencies=[Depends(auth)])
async def rewrite_post(pid:int):
    with db() as con:
        row=con.execute("SELECT * FROM posts WHERE id=? AND status='draft'",(pid,)).fetchone()
        if not row: raise HTTPException(404,'Черновик не найден')
        channel=con.execute('SELECT * FROM channels WHERE id=?',(row['channel_id'],)).fetchone()
    context=row['source_summary'] or row['text'].replace('⚠️ Требуется обработка ИИ перед публикацией.','').strip()
    try:
        if not interesting(row['title'] or '',context,channel):
            verdict=Verdict(publishable=False,score=0,reason='Не соответствует ключевым словам или исключающим фильтрам канала',text='')
        else:
            verdict=await review(row['title'],context,channel['editorial_brief'],draft=clean_post(row['text']) if row['user_edited'] else None,topic=channel['topic'],draft_headline=row['headline'])
    except EditorialUnavailable as exc:
        with db() as con:
            con.execute("UPDATE posts SET ai_status='pending',ai_text_hash='',ai_reason=?,review_attempts=review_attempts+1,review_kind=?,review_after=? WHERE id=? AND status='draft'",
                (str(exc),exc.kind,(utcnow()+timedelta(minutes=5 if exc.kind=='connection' else (60 if row['review_attempts']>=2 else 15))).isoformat(),pid))
        raise HTTPException(503,{'message':str(exc),'kind':exc.kind})
    state,text,reason,score=ai_result(verdict,row['url'],channel['min_score'])
    if not text: text=clean_post(row['text'])
    async with PUBLISH_LOCK:
        with db() as con:
            latest=con.execute('SELECT editorial_brief,min_score,keywords,exclude_keywords,topic FROM channels WHERE id=?',(channel['id'],)).fetchone()
            if any(latest[k]!=channel[k] for k in ('editorial_brief','min_score','keywords','exclude_keywords','topic')):
                raise HTTPException(409,'Настройки канала изменены во время оценки. Повторите.')
            cur=con.execute("UPDATE posts SET text=?,ai_status=?,ai_reason=?,ai_score=?,ai_text_hash=?,reviewed_at=?,editorial_revision=?,headline=?,news_event=?,news_evidence=?,user_edited=0,review_attempts=review_attempts+1,review_after=NULL,review_kind=NULL,error=NULL WHERE id=? AND status='draft' AND text=?",
                (text,state,reason,score,text_hash(text) if state=='approved' else '',utcnow().isoformat(),NEWS_REVISION,verdict.headline,verdict.event,verdict.evidence,pid,row['text']))
            if not cur.rowcount: raise HTTPException(409,'Черновик изменён во время оценки. Повторите.')
    event(state,reason,channel['id'],pid)
    return {'ok':True,'ai_status':state,'reason':reason}


@app.post('/api/automation/pause',dependencies=[Depends(auth)])
async def pause_automation():
    with db() as con: con.execute('UPDATE channels SET autopublish=0,next_publish_at=NULL')
    event('automation','Автопубликация остановлена для всех каналов')
    return {'ok':True}


@app.get('/api/events',dependencies=[Depends(auth)])
def events():
    with db() as con: return [dict(row) for row in con.execute('SELECT * FROM events ORDER BY id DESC LIMIT 50')]


@app.get('/api/overview',dependencies=[Depends(auth)])
def overview():
    with db() as con:
        counts=dict(con.execute("SELECT CASE WHEN status='draft' THEN ai_status ELSE status END,COUNT(*) FROM posts WHERE status!='deleted' GROUP BY 1").fetchall())
        counts['sources']=con.execute('SELECT COUNT(*) FROM sources').fetchone()[0]
        counts['automatic_channels']=con.execute('SELECT COUNT(*) FROM channels WHERE enabled=1 AND autopublish=1').fetchone()[0]
    counts['approved']=sum(len(ready_posts(c['id'])) for c in channels())
    counts['queue_target']=QUEUE_TARGET
    return counts


class ResolveIn(BaseModel):
    published: bool
    parts_removed: bool = False


@app.post('/api/posts/{pid}/resolve',dependencies=[Depends(auth)])
async def resolve_post(pid:int,item:ResolveIn):
    async with PUBLISH_LOCK:
        with db() as con:
            row=con.execute('SELECT remote_ids FROM posts WHERE id=?',(pid,)).fetchone()
            if row and json.loads(row['remote_ids'] or '[]') and not item.published and not item.parts_removed:
                raise HTTPException(409,'Часть новости уже опубликована. Удалите её в MAX и подтвердите это перед повторной отправкой.')
            cur=con.execute("UPDATE posts SET status=?,published_at=?,error=NULL WHERE id=? AND status='uncertain'",
                ('published' if item.published else 'draft',utcnow().isoformat() if item.published else None,pid))
            if not cur.rowcount: raise HTTPException(404,'Нет отправки, требующей сверки')
    event('resolved','Результат отправки подтверждён пользователем',post_id=pid)
    return {'ok':True}

async def review_pending(enabled_only=False,include_rejected=False,channel_id=None,target=QUEUE_TARGET,auto_send=True):
    try:
        with db() as con:
            query="SELECT p.id,p.channel_id,p.source_published_at,p.user_edited FROM posts p JOIN channels c ON c.id=p.channel_id WHERE p.status='draft' AND " + ("p.ai_status IN ('pending','rejected')" if include_rejected else "p.ai_status='pending'")
            query+=' AND (p.review_after IS NULL OR p.review_after<=?) AND (p.user_edited=1 OR (p.source_published_at>=? AND p.source_published_at<=?))'
            params=[utcnow().isoformat(),(utcnow()-timedelta(hours=48)).isoformat(),(utcnow()+timedelta(minutes=15)).isoformat()]
            if enabled_only: query+=' AND c.enabled=1'
            if channel_id is not None:
                query+=' AND p.channel_id=?';params.append(channel_id)
            rows=con.execute(query+' ORDER BY p.user_edited DESC,p.source_published_at DESC,p.id DESC LIMIT 60',params).fetchall()
        REVIEW_STATE.update(total=len(rows),done=0,error=None)
        processed=set()
        while len(processed)<60:
            # Re-read newest candidates after each model call; arrivals during slow
            # inference take priority over the older batch we started with.
            exclusion=' AND p.id NOT IN ('+','.join('?' for _ in processed)+')' if processed else ''
            params[:3]=[utcnow().isoformat(),(utcnow()-timedelta(hours=48)).isoformat(),(utcnow()+timedelta(minutes=15)).isoformat()]
            with db() as con:
                row=con.execute(query+exclusion+' ORDER BY (c.enabled AND c.autopublish) DESC,p.user_edited DESC,p.source_published_at DESC,p.id DESC LIMIT 1',params+list(processed)).fetchone()
            if not row: break
            processed.add(row['id'])
            REVIEW_STATE['total']=max(REVIEW_STATE['total'],len(processed))
            with db() as con: channel=con.execute('SELECT * FROM channels WHERE id=?',(row['channel_id'],)).fetchone()
            instant=bool(channel['enabled'] and channel['autopublish'])
            ready=prepared_posts(row['channel_id'])
            required=target+2 if target==QUEUE_TARGET else target
            if not instant and len(ready)>=required and not row['user_edited']:
                oldest=ready[-1]['source_published_at']
                if not row['source_published_at'] or row['source_published_at']<=oldest: continue
            if COLLECT_STATE['running']: COLLECT_STATE['phase']='Проверяем свежие новости: '+channel['name']
            try:
                await rewrite_post(row['id'])
                if auto_send: await automation_tick(channel_id=row['channel_id'])
                if not instant: trim_reserve(row['channel_id'],target=QUEUE_TARGET)
            except HTTPException as exc:
                detail=exc.detail
                if exc.status_code==503:
                    message=detail.get('message','Ошибка редактора') if isinstance(detail,dict) else str(detail)
                    REVIEW_STATE['error']=message
                    if isinstance(detail,dict) and detail.get('kind')=='connection': break
                    # A bad generation/fragment on one item must not stop the next good news item.
            REVIEW_STATE['done']+=1
    finally: REVIEW_STATE['running']=False


class ReviewBatchIn(BaseModel):
    include_rejected: bool = False
    channel_id: Optional[int] = Field(default=None,ge=1)


@app.post('/api/review',dependencies=[Depends(auth)])
async def start_review(item: Optional[ReviewBatchIn] = None):
    global REVIEW_TASK
    if REVIEW_STATE['running'] or COLLECT_STATE['running']: raise HTTPException(409,'Дождитесь завершения сбора или оценки')
    REVIEW_STATE.update(running=True,done=0,total=0,error=None)
    options=item or ReviewBatchIn()
    REVIEW_TASK=asyncio.create_task(review_pending(include_rejected=options.include_rejected,channel_id=options.channel_id))
    return {'started':True}


@app.get('/api/review/status',dependencies=[Depends(auth)])
def review_status():
    return dict(REVIEW_STATE)


def queue_review():
    global REVIEW_TASK
    if not REVIEW_STATE['running']:
        REVIEW_STATE.update(running=True,error=None)
        REVIEW_TASK=asyncio.create_task(review_pending())


async def manual_pipeline():
    await run_collection()
    if not REVIEW_STATE['running']:
        REVIEW_STATE['running']=True
        await review_pending()


def best_post(cid):
    now=utcnow()
    with db() as con:
        channel=con.execute('SELECT * FROM channels WHERE id=?',(cid,)).fetchone()
        posts=con.execute("SELECT * FROM posts WHERE channel_id=? AND status='draft' AND ai_status='approved' ORDER BY source_published_at DESC,ai_score DESC,id DESC",(cid,)).fetchall()
    return next((p for p in posts if auto_candidate(p,channel,now)),None)


async def publish_one_job(cid):
    job=ONE_JOBS[cid]
    try:
        candidate=best_post(cid)
        if not candidate:
            job['phase']='Ищем свежую новость и проверяем факты по RSS…'
            await run_collection(channel_id=cid,stop_after_ready=True,auto_send=False)
            candidate=best_post(cid)
            if not candidate:
                await review_pending(channel_id=cid,auto_send=False)
                candidate=best_post(cid)
        if not candidate:
            raise ValueError('Подходящей свежей новости по тематике канала нет. Статьи, инструкции и непроверенные тексты не публикуем.')
        job['phase']='Отправляем новость в MAX…'
        await publish(candidate['id'])
        job.update(post_id=candidate['id'],phase='Новость опубликована')
    except Exception as exc:
        job.update(error=str(exc) if isinstance(exc,(ValueError,HTTPException)) else 'Ошибка отправки. Проверьте журнал и канал MAX.',phase='Отправка не выполнена')
    finally:
        job.update(running=False,finished=utcnow().isoformat())


@app.post('/api/channels/{cid}/publish-one',dependencies=[Depends(auth)],status_code=202)
async def publish_one(cid:int):
    with db() as con: channel=con.execute('SELECT * FROM channels WHERE id=?',(cid,)).fetchone()
    if not channel: raise HTTPException(404,'Канал не найден')
    if not TOKEN or not channel['chat_id']: raise HTTPException(400,'Один раз укажите Chat ID канала в настройках и проверьте MAX_TOKEN')
    if ONE_JOBS.get(cid,{}).get('running'): raise HTTPException(409,'Уже готовим одну новость для этого канала')
    with db() as con:
        if con.execute("SELECT 1 FROM posts WHERE channel_id=? AND status='uncertain'",(cid,)).fetchone():
            raise HTTPException(409,'Сначала сверьте результат предыдущей отправки с MAX, чтобы не создать дубль')
    ONE_JOBS[cid]={'running':True,'phase':'Выбираем самую свежую подходящую новость…','error':None,'post_id':None,'finished':None}
    ONE_TASKS[cid]=asyncio.create_task(publish_one_job(cid))
    return {'started':True}


@app.get('/api/publish-jobs',dependencies=[Depends(auth)])
def publish_jobs():
    return {str(cid):dict(job) for cid,job in ONE_JOBS.items()}


def prepared_posts(cid,include_standby=True):
    now=utcnow()
    with db() as con:
        channel=con.execute('SELECT * FROM channels WHERE id=?',(cid,)).fetchone()
        if not channel: return []
        states="('approved','standby')" if include_standby else "('approved')"
        rows=con.execute("SELECT * FROM posts WHERE channel_id=? AND status='draft' AND ai_status IN "+states+" ORDER BY source_published_at DESC,ai_score DESC,id DESC",(cid,)).fetchall()
    result=[]
    for row in rows:
        candidate=dict(row);candidate['ai_status']='approved'
        if not auto_candidate(candidate,channel,now): continue
        title=re.sub(r'\W+',' ',row['title'] or '').casefold()
        event_name=re.sub(r'\W+',' ',row['news_event'] or '').casefold()
        if any(same_event(row['title'],p['title']) or same_event(row['news_event'],p['news_event']) for p in result): continue
        result.append(row)
    return result


def ready_posts(cid):
    return prepared_posts(cid,include_standby=False)


def same_event(left,right):
    left=re.sub(r'\W+',' ',left or '').strip().casefold()
    right=re.sub(r'\W+',' ',right or '').strip().casefold()
    if not left or not right: return False
    if set(re.findall(r'\d+',left))!=set(re.findall(r'\d+',right)): return False
    return left==right or (len(left)>20 and SequenceMatcher(None,left,right).ratio()>.94)


def trim_reserve(cid,target=QUEUE_TARGET):
    ready=ready_posts(cid)
    if len(ready)>target:
        with db() as con:
            con.executemany("UPDATE posts SET ai_status='standby' WHERE id=?",[(p['id'],) for p in ready[target:]])


def refresh_reserve():
    now=utcnow()
    with db() as con:
        con.execute("UPDATE posts SET ai_status='expired',ai_reason='Срок актуальности истёк — не отправляем' WHERE status='draft' AND ai_status IN ('approved','standby') AND (source_published_at<? OR created_at<?)",
            ((now-timedelta(hours=48)).isoformat(),(now-timedelta(hours=48)).strftime('%Y-%m-%d %H:%M:%S')))
        con.execute("UPDATE posts SET ai_status='pending',ai_text_hash='',review_after=NULL,ai_reason='Обновляем проверку перед отправкой' WHERE status='draft' AND ai_status IN ('approved','standby') AND reviewed_at<?",((now-timedelta(hours=24)).isoformat(),))
        ids=[r['id'] for r in con.execute('SELECT id FROM channels')]
    for cid in ids:
        count=len(ready_posts(cid))
        with db() as con:
            channel=con.execute('SELECT * FROM channels WHERE id=?',(cid,)).fetchone()
            standby=con.execute("SELECT * FROM posts WHERE channel_id=? AND status='draft' AND ai_status='standby' ORDER BY source_published_at DESC",(cid,)).fetchall()
            for post in standby:
                if not (channel['enabled'] and channel['autopublish']) and count>=QUEUE_TARGET: break
                candidate=dict(post);candidate['ai_status']='approved'
                if auto_candidate(candidate,channel,now):
                    con.execute("UPDATE posts SET ai_status='approved' WHERE id=?",(post['id'],));count+=1


def request_refill(force=False):
    global COLLECT_TASK
    if not force and LAST_REFILL_AT and utcnow()-LAST_REFILL_AT<timedelta(seconds=FEED_POLL_SECONDS): return
    if BACKGROUND_ENABLED and not COLLECT_LOCK.locked() and (not COLLECT_TASK or COLLECT_TASK.done()):
        COLLECT_TASK=asyncio.create_task(tick())


async def maintain_reserve():
    refresh_reserve()
    with db() as con:
        ids=[r['id'] for r in con.execute('SELECT DISTINCT c.id FROM channels c JOIN sources s ON s.channel_id=c.id')]
    if any(len(ready_posts(cid))<QUEUE_TARGET for cid in ids): request_refill()


@app.get('/api/queue/status',dependencies=[Depends(auth)])
def queue_status():
    result=[]
    with db() as con:
        rows=con.execute('SELECT c.*,COUNT(s.id) AS source_count FROM channels c LEFT JOIN sources s ON s.channel_id=c.id GROUP BY c.id').fetchall()
        errors=[dict(r) for r in con.execute('SELECT channel_id,url,last_error,last_checked_at FROM sources WHERE last_error IS NOT NULL')]
    for channel in rows:
        if not channel['source_count'] and not channel['chat_id'] and channel['id']!=1: continue
        count=len(ready_posts(channel['id']))
        result.append({'channel_id':channel['id'],'name':channel['name'],'ready':count,'target':QUEUE_TARGET,'missing':max(0,QUEUE_TARGET-count)})
    return {'channels':result,'running':COLLECT_STATE['running'] or REVIEW_STATE['running'],'source_errors':errors,'target':QUEUE_TARGET,'last_error':REVIEW_STATE.get('error'),'mode':'instant','poll_seconds':FEED_POLL_SECONDS}
