from __future__ import annotations
import os, sqlite3, hashlib, secrets, asyncio, re, html
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import calendar
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
from editorial import review, Verdict, EditorialUnavailable, text_hash, validate_copy, DEFAULT_EDITORIAL_BRIEF, LEGACY_BRIEFS
from apscheduler.schedulers.asyncio import AsyncIOScheduler

load_dotenv(Path(__file__).with_name(".env"))
DB = Path(os.getenv('NETWORK_DB_PATH', str(Path(__file__).with_name('network.db'))))
API = os.getenv('MAX_API_BASE', 'https://platform-api.max.ru').rstrip('/')
TOKEN = os.getenv('MAX_TOKEN', '')
PASSWORD = os.getenv('ADMIN_PASSWORD', '')
COLLECT_STATE = {'running': False, 'phase': 'Ожидание', 'found': 0, 'duplicates': 0, 'added': 0, 'errors': [], 'finished': None}
COLLECT_LOCK = asyncio.Lock()
COLLECT_TASK = None
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
        channel_fields = {
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
            'ai_status': "TEXT NOT NULL DEFAULT 'pending'", 'ai_score': 'INTEGER',
            'ai_reason': "TEXT NOT NULL DEFAULT 'Ещё не проверено редактором'",
            'ai_text_hash': "TEXT NOT NULL DEFAULT ''", 'source_summary': "TEXT NOT NULL DEFAULT ''",
            'source_published_at': 'TEXT', 'reviewed_at': 'TEXT', 'published_chat_id': 'TEXT',
            'publish_mode': 'TEXT',
        }
        for name, definition in post_fields.items():
            if name not in columns: con.execute(f'ALTER TABLE posts ADD COLUMN {name} {definition}')
        con.execute('CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, message TEXT NOT NULL, channel_id INTEGER, post_id INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP)')
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
        if con.execute('SELECT COUNT(*) FROM channels').fetchone()[0]:
            return
        for cid, name in [(1,'Нейросети'),(2,'Технологии')]:
            con.execute('INSERT OR IGNORE INTO channels(id,name,chat_id) VALUES (?,?,?)',(cid,name,os.getenv(f'MAX_CHAT_ID_{cid}','')))

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
    chat_id: str = ''
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
    if text and url: text+='\n\nИсточник: '+url
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

def feed_image(entry):
    candidates=[]
    for item in entry.get('media_content',[]) + entry.get('media_thumbnail',[]):
        if isinstance(item,dict): candidates.append(item.get('url',''))
    for item in entry.get('enclosures',[]):
        if str(item.get('type','')).startswith('image/'): candidates.append(item.get('href',''))
    for key in ('image','thumbnail'):
        v=entry.get(key)
        if isinstance(v,dict): candidates.append(v.get('href') or v.get('url'))
        elif isinstance(v,str): candidates.append(v)
    # RSS feeds sometimes embed the lead image in the HTML summary/content.
    for block in [entry.get('summary', '')] + [x.get('value', '') for x in entry.get('content', []) if isinstance(x, dict)]:
        candidates.extend(html.unescape(m.group(1)) for m in re.finditer(r'<img\b[^>]*?\bsrc=[\"\']([^\"\']+)', block, re.I))
    for url in candidates:
        checked=safe_image_url(urljoin(entry.get('link', ''), str(url or '')))
        if checked: return checked
    return ''

def feed_summary(entry):
    blocks=[entry.get('summary','')]+[item.get('value','') for item in entry.get('content',[]) if isinstance(item,dict)]
    cleaned=[]
    for block in blocks:
        value=re.sub(r'<(script|style)\b[^>]*>.*?</\1>', '', str(block or ''), flags=re.S|re.I)
        value=html.unescape(re.sub('<[^>]+>',' ',value))
        value=re.sub(r'\s+',' ',value).strip()
        if value: cleaned.append(value)
    return max(cleaned,key=len)[:6000] if cleaned else ''


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
    if not item.name.strip() or len(item.name)>120: raise HTTPException(400,'Название: 1–120 символов')
    if item.chat_id and not re.fullmatch(r'-?\d+',item.chat_id.strip()): raise HTTPException(400,'Chat ID должен быть числом')
    if item.start_hour>=item.end_hour: raise HTTPException(400,'Начало окна должно быть раньше окончания')
    try: ZoneInfo(item.timezone)
    except Exception: raise HTTPException(400,'Неизвестный часовой пояс')
    if not item.editorial_brief.strip() or len(item.editorial_brief)>2000: raise HTTPException(400,'Задайте тематику канала: 1–2000 символов')

async def collect(channel_id, stats=None):
    with db() as con:
        sources = con.execute('SELECT url FROM sources WHERE channel_id=?', (channel_id,)).fetchall()
        channel = con.execute('SELECT * FROM channels WHERE id=?', (channel_id,)).fetchone()
    added = 0
    async with httpx.AsyncClient(timeout=25, follow_redirects=True) as client:
        for source in sources:
            feed_url = source['url']
            if stats is not None:
                stats['phase'] = 'Читаем RSS: ' + feed_url[:90]
            try:
                response = await client.get(feed_url, headers={'User-Agent': 'Mozilla/5.0 (compatible; NeuroPulseRSS/1.0)'})
                response.raise_for_status()
                feed = feedparser.parse(response.content)
                if feed.bozo and not feed.entries:
                    raise ValueError('Некорректный RSS/Atom')
                if not feed.entries:
                    raise ValueError('RSS не содержит записей')
                for entry in feed.entries[:10]:
                    url = entry.get('link', '')
                    if not url.startswith(('https://', 'http://')):
                        continue
                    if stats is not None:
                        stats['found'] += 1
                    url = canonical_url(url)
                    fingerprint = hashlib.sha256((str(channel_id) + '|' + url).encode()).hexdigest()
                    title = html.unescape(re.sub('<[^>]+>', '', entry.get('title', '')))[:250]
                    summary = feed_summary(entry)
                    with db() as con:
                        existing=con.execute('SELECT id,status,source_summary FROM posts WHERE channel_id=? AND (fingerprint=? OR url=?)', (channel_id, fingerprint, url)).fetchone()
                        if existing:
                            if existing['status']=='draft' and summary and (not existing['source_summary'] or len(summary)>len(existing['source_summary'])+200):
                                con.execute("UPDATE posts SET source_summary=?,ai_status='pending',ai_score=NULL,ai_text_hash='',reviewed_at=NULL,ai_reason='Получено более полное описание RSS — нужна повторная оценка' WHERE id=?",(summary,existing['id']))
                            if stats is not None: stats['duplicates'] += 1
                            continue
                    if not interesting(title, summary, channel):
                        if stats is not None: stats['filtered'] += 1
                        continue
                    if stats is not None:
                        stats['phase'] = 'Ollama: ' + title[:80]
                    published=entry.get('published_parsed') or entry.get('updated_parsed')
                    published_at=datetime.fromtimestamp(calendar.timegm(published),timezone.utc).isoformat() if published else None
                    if published_at and utcnow()-datetime.fromisoformat(published_at)>timedelta(hours=72):
                        ai_status,content,reason,score='rejected',title+'\n\n'+summary[:900], 'Старше 72 часов',0
                    else:
                        try:
                            verdict=await review(title,summary,channel['editorial_brief'])
                            ai_status,content,reason,score=ai_result(verdict,url,channel['min_score'])
                        except EditorialUnavailable as exc:
                            ai_status,content,reason,score='pending',title+'\n\n'+summary[:900],str(exc),None
                            if stats is not None: stats['errors'].append(title[:60]+': '+str(exc))
                    if not content: content=title+'\n\n'+summary[:900]+('\n\nИсточник: '+url)
                    with db() as con:
                        latest=con.execute('SELECT * FROM channels WHERE id=?',(channel_id,)).fetchone()
                        if any(latest[k]!=channel[k] for k in ('editorial_brief','keywords','exclude_keywords','min_score')):
                            ai_status,reason='pending','Настройки редактора изменились во время сбора — нужна повторная оценка'
                        cur=con.execute('INSERT OR IGNORE INTO posts(channel_id,title,text,url,fingerprint,image_url,ai_status,ai_score,ai_reason,ai_text_hash,source_summary,source_published_at,reviewed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                            (channel_id,title,content,url,fingerprint,feed_image(entry),ai_status,score,reason,text_hash(content) if ai_status=='approved' else '',summary,published_at,utcnow().isoformat()))
                        added+=cur.rowcount
                        if stats is not None:
                            stats['added']+=cur.rowcount
                            if ai_status=='rejected': stats['filtered']+=cur.rowcount
                    if cur.rowcount: event(ai_status,reason,channel_id,cur.lastrowid)
            except Exception as exc:
                error = feed_url + ': ' + str(exc)[:180]
                print('Feed error:', error, flush=True)
                if stats is not None: stats['errors'].append(error)
            finally:
                if stats is not None: stats['sources_done'] += 1
    return added

PUBLISH_LOCK = asyncio.Lock()

def automation_ready(channel, now):
    if not channel['enabled'] or not channel['autopublish'] or not TOKEN: return False
    if not channel['chat_id'] or channel['verified_chat_id']!=channel['chat_id']: return False
    if channel['automation_error']: return False
    local=now.astimezone(ZoneInfo(channel['timezone']))
    if not channel['start_hour']<=local.hour<channel['end_hour']: return False
    due=channel['next_publish_at']
    if not due or datetime.fromisoformat(due)>now: return False
    start=local.replace(hour=0,minute=0,second=0,microsecond=0).astimezone(timezone.utc).isoformat()
    with db() as con:
        if con.execute("SELECT 1 FROM posts WHERE channel_id=? AND status='uncertain'",(channel['id'],)).fetchone(): return False
        sent=con.execute("SELECT COUNT(*) FROM posts WHERE channel_id=? AND status='published' AND published_at>=?",(channel['id'],start)).fetchone()[0]
        recent=con.execute("SELECT published_at FROM posts WHERE channel_id=? AND status='published' ORDER BY published_at DESC LIMIT 1",(channel['id'],)).fetchone()
    if sent>=channel['daily_limit']: return False
    if recent and now-datetime.fromisoformat(recent['published_at'].replace('Z','+00:00'))<timedelta(minutes=channel['interval_minutes']): return False
    return True


def auto_candidate(row,channel,now):
    if row['ai_status']!='approved' or row['ai_score'] is None or row['ai_score']<channel['min_score']: return False
    if not interesting(row['title'] or '',row['source_summary'] or row['text'],channel): return False
    if row['ai_text_hash']!=text_hash(row['text']) or not row['reviewed_at'] or row['error']: return False
    if not validate_copy(row['text'].split('\n\nИсточник:')[0]): return False
    if now-datetime.fromisoformat(row['reviewed_at'])>timedelta(hours=24): return False
    created=datetime.fromisoformat(row['created_at'].replace(' ','T')).replace(tzinfo=timezone.utc)
    if now-created>timedelta(hours=72): return False
    if row['source_published_at'] and now-datetime.fromisoformat(row['source_published_at'])>timedelta(hours=72): return False
    with db() as con:
        previous=con.execute("SELECT title,url FROM posts WHERE channel_id=? AND status IN ('published','uncertain')",(channel['id'],)).fetchall()
    title=re.sub(r'\W+',' ',row['title'] or '').strip().casefold()
    for other in previous:
        other_title=re.sub(r'\W+',' ',other['title'] or '').strip().casefold()
        if row['url'] and other['url'] and canonical_url(row['url'])==canonical_url(other['url']): return False
        if title and title==other_title: return False
        if len(title)>15 and SequenceMatcher(None,title,other_title).ratio()>.9: return False
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
    if not channel['enabled']: raise ValueError('Канал отключён')
    if row['text'].startswith('⚠️ Требуется обработка ИИ'): raise ValueError('Сначала обработайте черновик через Ollama')
    if automatic and (not automation_ready(channel,now) or not auto_candidate(row,channel,now)):
        raise ValueError('Пост или расписание не прошли проверку автопубликации')
    if not automatic and row['image_url'] and not row['image_approved']:
        raise ValueError('Подтвердите права на изображение либо уберите его URL')
    payload={'text':row['text'][:4000]}
    if row['image_url'] and row['image_approved']:
        payload['attachments']=[{'type':'image','payload':{'url':row['image_url']}}]
    with db() as con:
        # Durable claim prevents retries after a restart or another worker.
        cur=con.execute("UPDATE posts SET status='publishing',publish_mode=?,published_chat_id=? WHERE id=? AND status='draft'",
                        ('auto' if automatic else 'manual',channel['chat_id'],post_id))
        if not cur.rowcount: raise ValueError('Отправка уже началась')
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response=await client.post(f'{API}/messages',params={'chat_id':channel['chat_id']},headers={'Authorization':TOKEN},json=payload)
            response.raise_for_status()
            data=response.json()
        remote_id=str(data.get('message',{}).get('body',{}).get('mid',''))
        if not remote_id: raise ValueError('MAX не вернул идентификатор сообщения')
        with db() as con:
            con.execute("UPDATE posts SET status='published',published_at=?,remote_id=?,error=NULL WHERE id=?",(now.isoformat(),remote_id,post_id))
            con.execute('UPDATE channels SET verified_chat_id=?,next_publish_at=?,automation_error=NULL WHERE id=? AND chat_id=?',
                (channel['chat_id'],(now+timedelta(minutes=channel['interval_minutes'])).isoformat(),channel['id'],channel['chat_id']))
        event('published','Пост отправлен '+('автоматически' if automatic else 'вручную'),channel['id'],post_id)
        return {'ok':True,'remote_id':remote_id}
    except BaseException as exc:
        certain=isinstance(exc,httpx.HTTPStatusError) and 400<=exc.response.status_code<500
        state='draft' if certain else 'uncertain'
        message=('MAX отклонил запрос' if certain else 'Результат отправки неизвестен. Проверьте канал MAX, затем подтвердите результат в панели.')
        with db() as con:
            con.execute('UPDATE posts SET status=?,error=? WHERE id=?',(state,message,post_id))
            if automatic: con.execute('UPDATE channels SET autopublish=0,automation_error=?,next_publish_at=NULL WHERE id=?',(message,channel['id']))
        event('error',message,channel['id'],post_id)
        raise


async def automation_tick():
    if PUBLISH_LOCK.locked(): return
    with db() as con:
        channels=con.execute('SELECT * FROM channels WHERE enabled=1 AND autopublish=1').fetchall()
    for channel in channels:
        now=utcnow()
        if not automation_ready(channel,now): continue
        with db() as con:
            candidates=con.execute("SELECT * FROM posts WHERE channel_id=? AND status='draft' AND ai_status='approved' ORDER BY ai_score DESC,id",(channel['id'],)).fetchall()
        for post in candidates:
            if not auto_candidate(post,channel,now): continue
            try: await publish(post['id'],automatic=True)
            except Exception: pass  # Durable error + pause are recorded by publish().
            break

async def run_collection(enabled_only=False):
    async with COLLECT_LOCK:
        COLLECT_STATE.update(running=True, phase='Начинаем сбор', found=0, duplicates=0,
                             added=0, filtered=0, sources_done=0, sources_total=0, errors=[], finished=None)
        try:
            with db() as con:
                query = 'SELECT id FROM channels' + (' WHERE enabled=1' if enabled_only else '')
                ids = [row['id'] for row in con.execute(query)]
                COLLECT_STATE['sources_total'] = sum(con.execute('SELECT COUNT(*) FROM sources WHERE channel_id=?', (cid,)).fetchone()[0] for cid in ids)
            for cid in ids:
                await collect(cid, COLLECT_STATE)
        except Exception as exc:
            COLLECT_STATE['errors'].append(type(exc).__name__ + ': ошибка сбора')
        finally:
            COLLECT_STATE.update(running=False, phase='Готово', finished=datetime.now(timezone.utc).isoformat())

async def tick():
    if not COLLECT_LOCK.locked() and not COLLECT_STATE['running'] and not REVIEW_STATE['running']:
        COLLECT_STATE['running'] = True
        await run_collection(enabled_only=True)
        if not REVIEW_STATE['running']:
            REVIEW_STATE['running']=True
            await review_pending(enabled_only=True)

@asynccontextmanager
async def lifespan(app):
    init()
    scheduler=AsyncIOScheduler(timezone='UTC')
    scheduler.add_job(tick,'interval',minutes=30,max_instances=1,coalesce=True)
    scheduler.add_job(automation_tick,'interval',minutes=1,max_instances=1,coalesce=True)
    scheduler.start()
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)
        for task in (COLLECT_TASK, REVIEW_TASK):
            if task and not task.done():
                task.cancel()
                try: await task
                except asyncio.CancelledError: pass
app=FastAPI(title='MAX Network',lifespan=lifespan)
app.mount('/static',StaticFiles(directory=Path(__file__).with_name('static')),name='static')

@app.get('/',response_class=HTMLResponse)
def home(_=Depends(auth)):
    return Path(__file__).with_name('templates').joinpath('index.html').read_text(encoding='utf-8')

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
        if item.autopublish:
            if not item.enabled or not TOKEN or not item.chat_id: raise HTTPException(400,'Включите канал и задайте Chat ID и токен')
            if con.execute("SELECT 1 FROM posts WHERE channel_id=? AND status='uncertain'",(cid,)).fetchone():
                raise HTTPException(400,'Сначала подтвердите результат неизвестной отправки в очереди')
            if not current or current['verified_chat_id']!=item.chat_id.strip():
                raise HTTPException(400,'Сначала отправьте один пост вручную в этот Chat ID. Это проверит права бота.')
        changed=current and (current['editorial_brief']!=item.editorial_brief or current['keywords']!=item.keywords or current['exclude_keywords']!=item.exclude_keywords or current['min_score']!=item.min_score)
        if changed:
            con.execute("UPDATE posts SET ai_status='pending',ai_text_hash='',ai_reason='Тематика канала изменена — нужна повторная оценка' WHERE channel_id=? AND status='draft'",(cid,))
        enabled_now=item.autopublish and (not current or not current['autopublish'])
        values['next_publish_at']=(utcnow()+timedelta(minutes=item.interval_minutes)).isoformat() if enabled_now else (current['next_publish_at'] if current and item.autopublish else None)
        values['automation_error']=None
        if current:
            if current['chat_id']!=item.chat_id.strip(): values['verified_chat_id']=''
            con.execute('UPDATE channels SET '+','.join(k+'=?' for k in values)+' WHERE id=?',list(values.values())+[cid])
        else:
            cur=con.execute('INSERT INTO channels('+','.join(values)+') VALUES ('+','.join('?' for _ in values)+')',list(values.values()))
            cid=cur.lastrowid
    if enabled_now: event('automation','Автопостинг включён; первая отправка не раньше заданного интервала',cid)
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
def add_source(item:SourceIn):
    url = item.url.strip()
    if not safe_image_url(url): raise HTTPException(400,'Укажите публичный HTTPS URL источника')
    with db() as con:
        if not con.execute('SELECT 1 FROM channels WHERE id=?',(item.channel_id,)).fetchone(): raise HTTPException(404,'Канал не найден')
        cur=con.execute('INSERT OR IGNORE INTO sources(channel_id,url) VALUES (?,?)',(item.channel_id,url))
        if not cur.rowcount: raise HTTPException(409,'Источник уже добавлен')
    return {'ok':True}
@app.delete('/api/sources/{sid}',dependencies=[Depends(auth)])
def remove_source(sid:int):
    with db() as con: con.execute('DELETE FROM sources WHERE id=?',(sid,))
    return {'ok':True}
@app.get('/api/posts',dependencies=[Depends(auth)])
def posts():
    with db() as con: return [dict(x) for x in con.execute("SELECT * FROM posts WHERE status!='deleted' ORDER BY id DESC LIMIT 200")]
@app.get('/api/collect/status', dependencies=[Depends(auth)])
def collect_status():
    return dict(COLLECT_STATE)

@app.post('/api/collect', dependencies=[Depends(auth)])
async def collect_now():
    global COLLECT_TASK
    if COLLECT_STATE['running'] or COLLECT_LOCK.locked() or REVIEW_STATE['running']:
        raise HTTPException(409, 'Сбор уже выполняется')
    COLLECT_STATE.update(running=True, phase='Начинаем сбор', errors=[], finished=None)
    COLLECT_TASK = asyncio.create_task(run_collection())
    return {'started':True}
@app.put('/api/posts/{pid}',dependencies=[Depends(auth)])
async def edit_post(pid:int,item:dict):
    if PUBLISH_LOCK.locked(): raise HTTPException(409,'Дождитесь завершения публикации')
    value=str(item.get('text','')).strip()
    if not value or len(value)>4000: raise HTTPException(400,'Text must be 1-4000 characters')
    image_url=str(item.get('image_url','')).strip()
    if image_url and not safe_image_url(image_url): raise HTTPException(400,'Image must be a public HTTPS URL')
    approved=int(bool(item.get('image_approved',False))) if image_url else 0
    with db() as con:
        row=con.execute("SELECT * FROM posts WHERE id=? AND status='draft'",(pid,)).fetchone()
        if not row: raise HTTPException(400,'Редактировать можно только черновики')
        changed=value!=row['text']
        con.execute("UPDATE posts SET text=?,image_url=?,image_approved=?,ai_status=?,ai_text_hash=?,ai_reason=?,error=NULL WHERE id=?",
            (value,image_url,approved,'pending' if changed else row['ai_status'],'' if changed else row['ai_text_hash'],
             'Текст изменён — нужна оценка ИИ' if changed else row['ai_reason'],pid))
    return {'ok':True}
@app.post('/api/posts/{pid}/publish',dependencies=[Depends(auth)])
async def publish_now(pid:int):
    try: return await publish(pid)
    except ValueError as exc: raise HTTPException(400,str(exc))
    except Exception: raise HTTPException(502,'Ошибка MAX API. Проверьте доступ, токен и права бота; перед повтором проверьте канал.')

@app.delete('/api/posts/{pid}',dependencies=[Depends(auth)])
async def delete_post(pid:int):
    async with PUBLISH_LOCK:
        with db() as con:
            # Retain fingerprint to prevent deleted stories from reappearing on the next collection.
            cur=con.execute("UPDATE posts SET status='deleted' WHERE id=? AND status='draft'",(pid,))
            if not cur.rowcount: raise HTTPException(404,'Черновик не найден')
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
            verdict=await review(row['title'],context,channel['editorial_brief'])
    except EditorialUnavailable as exc: raise HTTPException(503,str(exc))
    state,text,reason,score=ai_result(verdict,row['url'],channel['min_score'])
    if not text: text=row['text']
    async with PUBLISH_LOCK:
        with db() as con:
            latest=con.execute('SELECT editorial_brief,min_score,keywords,exclude_keywords FROM channels WHERE id=?',(channel['id'],)).fetchone()
            if any(latest[k]!=channel[k] for k in ('editorial_brief','min_score','keywords','exclude_keywords')):
                raise HTTPException(409,'Настройки канала изменены во время оценки. Повторите.')
            cur=con.execute("UPDATE posts SET text=?,ai_status=?,ai_reason=?,ai_score=?,ai_text_hash=?,reviewed_at=?,error=NULL WHERE id=? AND status='draft' AND text=?",
                (text,state,reason,score,text_hash(text) if state=='approved' else '',utcnow().isoformat(),pid,row['text']))
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
    return counts


class ResolveIn(BaseModel):
    published: bool


@app.post('/api/posts/{pid}/resolve',dependencies=[Depends(auth)])
async def resolve_post(pid:int,item:ResolveIn):
    async with PUBLISH_LOCK:
        with db() as con:
            cur=con.execute("UPDATE posts SET status=?,published_at=?,error=NULL WHERE id=? AND status='uncertain'",
                ('published' if item.published else 'draft',utcnow().isoformat() if item.published else None,pid))
            if not cur.rowcount: raise HTTPException(404,'Нет отправки, требующей сверки')
    event('resolved','Результат отправки подтверждён пользователем',post_id=pid)
    return {'ok':True}

async def review_pending(enabled_only=False, include_rejected=False, channel_id=None):
    try:
        with db() as con:
            query="SELECT p.id FROM posts p JOIN channels c ON c.id=p.channel_id WHERE p.status='draft' AND " + ("p.ai_status IN ('pending','rejected')" if include_rejected else "p.ai_status='pending'")
            if enabled_only: query+=' AND c.enabled=1'
            params=[]
            if channel_id is not None:
                query+=' AND p.channel_id=?'
                params.append(channel_id)
            ids=[row['id'] for row in con.execute(query+' ORDER BY p.id LIMIT 30',params)]
        REVIEW_STATE.update(total=len(ids),done=0,error=None)
        for pid in ids:
            try: await rewrite_post(pid)
            except HTTPException as exc:
                if exc.status_code==503:
                    REVIEW_STATE['error']=str(exc.detail)
                    break
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
