from __future__ import annotations
import os, sqlite3, hashlib, secrets, asyncio, re, html
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse, urljoin, urlsplit, urlunsplit, parse_qsl, urlencode
from typing import Optional
import feedparser, httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler

load_dotenv(Path(__file__).with_name(".env"))
DB = Path(os.getenv('NETWORK_DB_PATH', str(Path(__file__).with_name('network.db'))))
API = os.getenv('MAX_API_BASE', 'https://platform-api.max.ru').rstrip('/')
TOKEN = os.getenv('MAX_TOKEN', '')
PASSWORD = os.getenv('ADMIN_PASSWORD', '')
COLLECT_STATE = {'running': False, 'phase': 'Ожидание', 'found': 0, 'duplicates': 0, 'added': 0, 'errors': [], 'finished': None}
COLLECT_LOCK = asyncio.Lock()
COLLECT_TASK = None
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
class PostIn(BaseModel):
    channel_id: int
    text: str

async def rewrite(title, summary, url):
    """Use a local Ollama model by default; no VPN or paid API required."""
    model = os.getenv('OLLAMA_MODEL', 'qwen2.5:3b')
    base = os.getenv('OLLAMA_URL', 'http://127.0.0.1:11434').rstrip('/')
    prompt = (
        'Ты редактор русскоязычного канала об искусственном интеллекте. '
        'Напиши информативный пост на русском языке длиной 400–750 символов, включая заголовок, но без ссылки. '
        'Используй только факты из заголовка и описания; ничего не придумывай. '
        'Не выдавай рекламу за новость. Не повторяй английский текст. '
        'Дай короткий заголовок без кликбейта, затем 1–2 абзаца. Не добавляй хештеги. '
        'Ссылку добавит приложение, не вставляй её в ответ.\n\n'
        f'Заголовок: {title}\nОписание: {summary}\nИсточник: {url}'
    )
    try:
        async with httpx.AsyncClient(timeout=120, trust_env=False) as client:
            res = await client.post(
                f'{base}/api/generate',
                json={'model': model, 'prompt': prompt, 'stream': False,
                      'options': {'temperature': 0.3}},
            )
            res.raise_for_status()
            result = res.json().get('response', '').strip()
            if not result or not re.search('[А-Яа-яЁё]', result):
                raise ValueError('Ollama returned empty or untranslated text')
            result = result.replace(url, '').strip() if url else result
            if len(result)>750:
                result=result[:747].rsplit(' ',1)[0]+'…'
            return result + (f'\n\nИсточник: {url}' if url else '')
    except Exception as exc:
        print('Local AI unavailable:', type(exc).__name__)
        # Never silently turn an untranslated English feed into a publishable post.
        return (f'⚠️ Требуется обработка ИИ перед публикацией.\n\n'
                f'{title}\n\n{summary[:900]}\n\nИсточник: {url}')[:4000]

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
    if item.autopublish: raise HTTPException(400,'Автопубликация отключена до отдельной проверки')

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
                    with db() as con:
                        if con.execute('SELECT 1 FROM posts WHERE channel_id=? AND (fingerprint=? OR url=?)', (channel_id, fingerprint, url)).fetchone():
                            if stats is not None: stats['duplicates'] += 1
                            continue
                    title = html.unescape(re.sub('<[^>]+>', '', entry.get('title', '')))[:250]
                    summary = html.unescape(re.sub('<[^>]+>', '', entry.get('summary', '')))[:1200]
                    if not interesting(title, summary, channel):
                        if stats is not None: stats['filtered'] += 1
                        continue
                    if stats is not None:
                        stats['phase'] = 'Ollama: ' + title[:80]
                    content = await rewrite(title, summary, url)
                    with db() as con:
                        cur = con.execute('INSERT OR IGNORE INTO posts(channel_id,title,text,url,fingerprint,image_url) VALUES (?,?,?,?,?,?)', (channel_id,title,content,url,fingerprint,feed_image(entry)))
                        added += cur.rowcount
                        if stats is not None: stats['added'] += cur.rowcount
            except Exception as exc:
                error = feed_url + ': ' + str(exc)[:180]
                print('Feed error:', error, flush=True)
                if stats is not None: stats['errors'].append(error)
            finally:
                if stats is not None: stats['sources_done'] += 1
    return added

PUBLISH_LOCK = asyncio.Lock()

async def publish(post_id):
    async with PUBLISH_LOCK:
        return await _publish(post_id)

async def _publish(post_id):
    with db() as con:
        row=con.execute('SELECT p.*,c.chat_id,c.enabled FROM posts p JOIN channels c ON c.id=p.channel_id WHERE p.id=?',(post_id,)).fetchone()
    if not row: raise ValueError('Post not found')
    if row['status'] != 'draft': raise ValueError('Post is not a draft')
    if row['text'].startswith('⚠️ Требуется обработка ИИ'): raise ValueError('Start local Ollama and edit/recreate this draft before publishing')
    if not TOKEN or not row['chat_id']: raise ValueError('MAX_TOKEN/chat_id not configured')
    if not row['enabled']: raise ValueError('Channel disabled')
    if row['image_url'] and not row['image_approved']: raise ValueError('Подтвердите права на использование изображения перед публикацией')
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            payload={'text':row['text'][:4000]}
            if row['image_url']:
                payload['attachments']=[{'type':'image','payload':{'url':row['image_url']}}]
            response=await client.post(f'{API}/messages',params={'chat_id':row['chat_id']},headers={'Authorization':TOKEN},json=payload)
            response.raise_for_status()
            data=response.json()
        with db() as con:
            con.execute('UPDATE posts SET status=?,published_at=?,remote_id=?,error=NULL WHERE id=?',('published',datetime.now(timezone.utc).isoformat(),str(data.get('message',{}).get('body',{}).get('mid','')),post_id))
        return data
    except Exception as exc:
        with db() as con:
            con.execute('UPDATE posts SET error=? WHERE id=?',('Ошибка MAX API: ' + type(exc).__name__,post_id))
        raise

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
    # Only collect drafts. Automatic publication is deliberately unavailable.
    if not COLLECT_LOCK.locked() and not COLLECT_STATE['running']:
        COLLECT_STATE['running'] = True
        await run_collection(enabled_only=True)

@asynccontextmanager
async def lifespan(app):
    init()
    scheduler=AsyncIOScheduler(timezone='UTC')
    scheduler.add_job(tick,'interval',hours=3,max_instances=1,coalesce=True)
    scheduler.start()
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)
        if COLLECT_TASK and not COLLECT_TASK.done():
            COLLECT_TASK.cancel()
            try:
                await COLLECT_TASK
            except asyncio.CancelledError:
                pass
app=FastAPI(title='MAX Network',lifespan=lifespan)

@app.get('/',response_class=HTMLResponse)
def home(_=Depends(auth)):
    return Path(__file__).with_name('templates').joinpath('index.html').read_text(encoding='utf-8')

@app.get('/api/channels',dependencies=[Depends(auth)])
def channels():
    with db() as con: return [dict(x) for x in con.execute('SELECT * FROM channels')]
@app.put('/api/channels/{cid}',dependencies=[Depends(auth)])
def update_channel(cid:int,item:ChannelIn):
    if PUBLISH_LOCK.locked(): raise HTTPException(409,'Дождитесь завершения публикации')
    validate_channel(item)
    with db() as con:
        cur=con.execute('UPDATE channels SET name=?,chat_id=?,enabled=?,autopublish=0,keywords=?,exclude_keywords=? WHERE id=?',
                        (item.name.strip(),item.chat_id.strip(),int(item.enabled),item.keywords,item.exclude_keywords,cid))
        if not cur.rowcount: raise HTTPException(404, 'Канал не найден')
    return {'ok':True}

@app.post('/api/channels',dependencies=[Depends(auth)])
def create_channel(item:ChannelIn):
    validate_channel(item)
    with db() as con:
        cur=con.execute('INSERT INTO channels(name,chat_id,enabled,keywords,exclude_keywords) VALUES (?,?,?,?,?)',
                        (item.name.strip(),item.chat_id.strip(),int(item.enabled),item.keywords,item.exclude_keywords))
    return {'id':cur.lastrowid}

@app.delete('/api/channels/{cid}',dependencies=[Depends(auth)])
def delete_channel(cid:int):
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
    with db() as con: return [dict(x) for x in con.execute("SELECT * FROM posts WHERE status!='deleted' ORDER BY id DESC LIMIT 100")]
@app.get('/api/collect/status', dependencies=[Depends(auth)])
def collect_status():
    return dict(COLLECT_STATE)

@app.post('/api/collect', dependencies=[Depends(auth)])
async def collect_now():
    global COLLECT_TASK
    if COLLECT_STATE['running'] or COLLECT_LOCK.locked():
        raise HTTPException(409, 'Сбор уже выполняется')
    COLLECT_STATE.update(running=True, phase='Начинаем сбор', errors=[], finished=None)
    COLLECT_TASK = asyncio.create_task(run_collection())
    return {'started':True}
@app.put('/api/posts/{pid}',dependencies=[Depends(auth)])
def edit_post(pid:int,item:dict):
    if PUBLISH_LOCK.locked(): raise HTTPException(409,'Дождитесь завершения публикации')
    value=str(item.get('text','')).strip()
    if not value or len(value)>4000: raise HTTPException(400,'Text must be 1-4000 characters')
    image_url=str(item.get('image_url','')).strip()
    if image_url and not safe_image_url(image_url): raise HTTPException(400,'Image must be a public HTTPS URL')
    approved=int(bool(item.get('image_approved',False))) if image_url else 0
    with db() as con:
        cur=con.execute("UPDATE posts SET text=?,image_url=?,image_approved=? WHERE id=? AND status='draft'",(value,image_url,approved,pid))
        if not cur.rowcount: raise HTTPException(400,'Only drafts can be edited')
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
    result={'max_token_configured':bool(TOKEN),'ollama':False,'model':os.getenv('OLLAMA_MODEL','qwen2.5:3b'),'autopublish':False}
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
    if PUBLISH_LOCK.locked(): raise HTTPException(409,'Дождитесь завершения публикации')
    with db() as con:
        row=con.execute("SELECT * FROM posts WHERE id=? AND status='draft'",(pid,)).fetchone()
    if not row: raise HTTPException(404,'Черновик не найден')
    # Reuse the available draft as source context, removing only the failure marker.
    context=row['text'].replace('⚠️ Требуется обработка ИИ перед публикацией.', '').strip()
    text=await rewrite(row['title'],context,row['url'] or '')
    if text.startswith('⚠️ Требуется обработка ИИ'):
        raise HTTPException(503,'Ollama недоступна или модель не установлена. Запустите ollama serve и загрузите qwen2.5:3b.')
    async with PUBLISH_LOCK:
        with db() as con:
            cur=con.execute("UPDATE posts SET text=?,error=NULL WHERE id=? AND status='draft' AND text=?",(text,pid,row['text']))
            if not cur.rowcount: raise HTTPException(409,'Черновик изменён во время генерации; повторите операцию')
    return {'ok':True}
