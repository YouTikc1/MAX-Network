import asyncio
import base64
import sqlite3
import time
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient
import app as module

ASYNC_CLIENT = httpx.AsyncClient
AUTH = {'Authorization': 'Basic ' + base64.b64encode(b'admin:test-only-password').decode()}

@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(module, 'DB', tmp_path / 'network.db')
    monkeypatch.setattr(module, 'PASSWORD', 'test-only-password')
    monkeypatch.setattr(module, 'TOKEN', 'test-only-token')
    monkeypatch.setattr(module, 'COLLECT_LOCK', asyncio.Lock())
    monkeypatch.setattr(module, 'PUBLISH_LOCK', asyncio.Lock())
    monkeypatch.setattr(module, 'COLLECT_TASK', None)
    module.COLLECT_STATE.update(running=False, errors=[], finished=None)
    with TestClient(module.app, headers=AUTH) as c:
        yield c

def add_post(**changes):
    values = dict(channel_id=1, title='Новая модель ИИ', text='Короткий русский пост', url='https://news.example/story', fingerprint='story-1')
    values.update(changes)
    with module.db() as con:
        cols=','.join(values)
        cur=con.execute(f"INSERT INTO posts({cols}) VALUES ({','.join('?' for _ in values)})",list(values.values()))
        return cur.lastrowid

def mock_http(monkeypatch, handler):
    original=ASYNC_CLIENT
    monkeypatch.setattr(module.httpx, 'AsyncClient', lambda *a, **kw: original(transport=httpx.MockTransport(handler), **kw))

def test_auth_and_html(client):
    assert client.get('/api/sources',headers={'Authorization':''}).status_code == 401
    response=client.get('/')
    assert response.status_code == 200
    assert 'data-rewrite' in response.text
    assert 'sourceChannel' in response.text
    assert '<script>' in response.text

def test_channels_sources_crud(client):
    channel=client.post('/api/channels',json={'name':'NeuroPulse','chat_id':'-1234'}).json()['id']
    assert channel > 2
    payload={'channel_id':channel,'url':'https://news.example/rss'}
    assert client.post('/api/sources',json=payload).status_code == 200
    assert client.post('/api/sources',json=payload).status_code == 409
    source=client.get('/api/sources').json()[0]
    assert source['channel_id'] == channel
    assert client.delete(f'/api/channels/{channel}').status_code == 409
    assert client.delete('/api/sources/'+str(source['id'])).status_code == 200
    assert client.delete(f'/api/channels/{channel}').status_code == 200
    assert client.post('/api/sources',json=payload).status_code == 404

@pytest.mark.parametrize('url',['https://127.0.0.1/rss','https://10.0.0.1/rss','https://example.com:bad/rss','https://u:p@example.com/rss','javascript:alert(1)'])
def test_source_validation(client,url):
    assert client.post('/api/sources',json={'channel_id':1,'url':url}).status_code == 400

def test_autopublish_disabled(client):
    assert client.put('/api/channels/1',json={'name':'Test','autopublish':True}).status_code == 400
    assert client.put('/api/channels/1',json={'name':'Test','enabled':True}).status_code == 200
    assert client.get('/api/channels').json()[0]['autopublish'] == 0

def test_draft_edit_delete_keeps_history(client):
    pid=add_post()
    assert client.put(f'/api/posts/{pid}',json={'text':'Обновлённый текст'}).status_code == 200
    assert client.get('/api/posts').json()[0]['text']=='Обновлённый текст'
    assert client.delete(f'/api/posts/{pid}').status_code == 200
    assert client.get('/api/posts').json()==[]
    with module.db() as con:
        assert con.execute('SELECT fingerprint,status FROM posts WHERE id=?',(pid,)).fetchone()['status']=='deleted'

def test_collection_generation_images_filters_and_dedup(client,monkeypatch):
    rss='''<rss version="2.0"><channel><title>Test</title>
    <item><title>Новая модель ИИ</title><link>https://news.example/story?utm_source=rss</link><description><![CDATA[<p>Описание</p><img src="/lead.jpg">]]></description></item>
    <item><title>Реклама ИИ</title><link>https://news.example/ad</link></item>
    <item><title>Спорт</title><link>https://news.example/sport</link></item>
    </channel></rss>'''
    calls=[]
    def handler(request):
        calls.append(str(request.url))
        if request.url.path=='/api/generate':
            return httpx.Response(200,json={'response':'Короткий пост о новой модели на русском языке.'})
        return httpx.Response(200,text=rss)
    mock_http(monkeypatch,handler)
    client.put('/api/channels/1',json={'name':'NeuroPulse','enabled':True,'keywords':'ИИ','exclude_keywords':'реклама'})
    client.post('/api/sources',json={'channel_id':1,'url':'https://news.example/rss'})
    for run in range(2):
        assert client.post('/api/collect').json()['started']
        for _ in range(200):
            state=client.get('/api/collect/status').json()
            if not state['running']: break
            time.sleep(.005)
        assert not state['running']
        assert not state['errors']
        assert state['sources_done']==1 and state['sources_total']==1
        assert state['added']==(1 if run==0 else 0)
        assert state['filtered']==2
        assert state['duplicates']==run
    posts=client.get('/api/posts').json()
    assert len(posts)==1
    assert posts[0]['image_url']=='https://news.example/lead.jpg'
    assert posts[0]['url']=='https://news.example/story'
    assert 'Источник: https://news.example/story' in posts[0]['text']
    assert len([x for x in calls if '/api/generate' in x])==1
    client.delete('/api/posts/'+str(posts[0]['id']))
    asyncio.run(module.run_collection())
    assert module.COLLECT_STATE['duplicates']==1

def test_collection_returns_immediately_and_prevents_overlap(client,monkeypatch):
    async def slow_collect(cid,stats):
        await asyncio.sleep(.2)
    monkeypatch.setattr(module,'collect',slow_collect)
    before=time.monotonic()
    assert client.post('/api/collect').status_code==200
    assert time.monotonic()-before < .3
    assert client.post('/api/collect').status_code==409

def test_ollama_failure_and_retry(client,monkeypatch):
    def failure(request):
        raise httpx.ConnectError('offline',request=request)
    mock_http(monkeypatch,failure)
    text=asyncio.run(module.rewrite('Title','Summary','https://news.example/story'))
    assert text.startswith('⚠️ Требуется обработка ИИ')
    pid=add_post(text=text)
    assert client.post(f'/api/posts/{pid}/publish').status_code==400
    assert client.post(f'/api/posts/{pid}/rewrite').status_code==503
    assert client.get('/api/posts').json()[0]['text']==text

def test_manual_max_publish_and_repeat_guard(client,monkeypatch):
    requests=[]
    def handler(request):
        requests.append(request)
        return httpx.Response(200,json={'message':{'body':{'mid':'remote-123'}}})
    mock_http(monkeypatch,handler)
    client.put('/api/channels/1',json={'name':'NeuroPulse','enabled':True,'chat_id':'-123'})
    pid=add_post(image_url='https://news.example/photo.jpg')
    assert client.post(f'/api/posts/{pid}/publish').status_code==400
    client.put(f'/api/posts/{pid}',json={'text':'Проверенный русский пост','image_url':'https://news.example/photo.jpg','image_approved':True})
    assert client.post(f'/api/posts/{pid}/publish').status_code==200
    assert client.post(f'/api/posts/{pid}/publish').status_code==400
    assert len(requests)==1
    assert requests[0].url.host=='platform-api.max.ru'
    assert requests[0].url.params['chat_id']=='-123'
    assert client.get('/api/posts').json()[0]['remote_id']=='remote-123'
    assert client.delete(f'/api/posts/{pid}').status_code==404

def test_scheduler_never_publishes(client,monkeypatch):
    with module.db() as con:
        con.execute('UPDATE channels SET enabled=1,autopublish=1')
    calls=[]
    async def collect(cid,stats): calls.append(cid)
    async def publish(pid): pytest.fail('Autopublish must not run')
    monkeypatch.setattr(module,'collect',collect)
    monkeypatch.setattr(module,'publish',publish)
    add_post()
    asyncio.run(module.tick())
    assert calls==[1,2]

def test_old_schema_migration_is_additive(tmp_path,monkeypatch):
    target=tmp_path/'old.db'
    with sqlite3.connect(target) as con:
        con.executescript('''CREATE TABLE channels (id INTEGER PRIMARY KEY, name TEXT NOT NULL, chat_id TEXT, enabled INTEGER DEFAULT 0, autopublish INTEGER DEFAULT 0);
        CREATE TABLE sources (id INTEGER PRIMARY KEY, channel_id INTEGER NOT NULL, url TEXT NOT NULL, UNIQUE(channel_id,url));
        CREATE TABLE posts (id INTEGER PRIMARY KEY, channel_id INTEGER NOT NULL, title TEXT, text TEXT NOT NULL, url TEXT, fingerprint TEXT UNIQUE, status TEXT NOT NULL DEFAULT 'draft', created_at TEXT DEFAULT CURRENT_TIMESTAMP, published_at TEXT, error TEXT, remote_id TEXT);
        INSERT INTO channels(id,name) VALUES (7,'Сохранённый канал');
        INSERT INTO sources(channel_id,url) VALUES (7,'https://news.example/rss');
        INSERT INTO posts(channel_id,text,fingerprint) VALUES (7,'Сохранённый текст','old');''')
    monkeypatch.setattr(module,'DB',target)
    module.init(); module.init()
    with module.db() as con:
        assert con.execute('SELECT COUNT(*) FROM channels').fetchone()[0]==1
        assert con.execute('SELECT text FROM posts').fetchone()[0]=='Сохранённый текст'
        assert con.execute('SELECT COUNT(*) FROM sources').fetchone()[0]==1
        assert 'keywords' in con.execute('SELECT * FROM channels').fetchone().keys()

def test_generated_text_is_short_and_russian(client,monkeypatch):
    mock_http(monkeypatch,lambda request:httpx.Response(200,json={'response':'Короткая новость о модели. '*100}))
    text=asyncio.run(module.rewrite('Title','Summary','https://news.example/story'))
    assert len(text.split('\n\nИсточник:')[0])<=750
    assert text.endswith('https://news.example/story')
    mock_http(monkeypatch,lambda request:httpx.Response(200,json={'response':'Only English text'}))
    assert asyncio.run(module.rewrite('Title','Summary','https://news.example/story')).startswith('⚠️ Требуется обработка ИИ')
