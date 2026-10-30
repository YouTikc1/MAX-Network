import asyncio
import base64
import sqlite3
import time
import json
from datetime import datetime, timezone, timedelta
import editorial
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient
import app as module

ASYNC_CLIENT = httpx.AsyncClient
COPY = 'Компания представила новую модель искусственного интеллекта. По данным разработчиков, она поддерживает работу с документами и изображениями. Исследователи опубликовали результаты тестов и описание возможностей системы.'

def verdict_response(approved=True,score=90,text=COPY,reason='Конкретный релиз новой модели с описанием возможностей'):
    return {'response':json.dumps({'publishable':approved,'score':score,'reason':reason,'text':text if approved else ''},ensure_ascii=False)}

AUTH = {'Authorization': 'Basic ' + base64.b64encode(b'admin:test-only-password').decode()}

@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(module, 'DB', tmp_path / 'network.db')
    monkeypatch.setattr(module, 'PASSWORD', 'test-only-password')
    monkeypatch.setattr(module, 'TOKEN', 'test-only-token')
    monkeypatch.setattr(module, 'COLLECT_LOCK', asyncio.Lock())
    monkeypatch.setattr(editorial, 'INFERENCE_LOCK', asyncio.Lock())
    monkeypatch.setattr(module, 'PUBLISH_LOCK', asyncio.Lock())
    monkeypatch.setattr(module, 'COLLECT_TASK', None)
    monkeypatch.setattr(module,'REVIEW_TASK',None)
    module.REVIEW_STATE.update(running=False,done=0,total=0,error=None)
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
    assert '/static/app.js' in response.text
    assert client.get('/static/app.js').status_code==200
    assert client.get('/static/app.css').status_code==200
    assert 'sourceChannel' in response.text
    assert '<script src=' in response.text

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
            return httpx.Response(200,json=verdict_response())
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
    with pytest.raises(editorial.EditorialUnavailable):
        asyncio.run(editorial.review('Title','Summary','ИИ'))
    pid=add_post()
    assert client.post(f'/api/posts/{pid}/rewrite').status_code==503
    assert client.get('/api/posts').json()[0]['ai_status']=='pending'
    assert client.get('/api/posts').json()[0]['text']=='Короткий русский пост'

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

@pytest.mark.parametrize('text',[COPY*10,'Only English text with no Russian words at all.', 'Меня зовут Дмитрий, я расскажу об этой модели. '+COPY,'### Заголовок\n'+COPY])
def test_generated_copy_rejects_bad_output(client,monkeypatch,text):
    mock_http(monkeypatch,lambda request:httpx.Response(200,json=verdict_response(text=text)))
    with pytest.raises(editorial.EditorialUnavailable):
        asyncio.run(editorial.review('Title','Summary','ИИ'))

def configure_auto(monkeypatch):
    now=datetime.now(timezone.utc).replace(microsecond=0)
    monkeypatch.setattr(module,'utcnow',lambda:now)
    with module.db() as con:
        con.execute("UPDATE channels SET chat_id='-123',verified_chat_id='-123',enabled=1,autopublish=1,start_hour=0,end_hour=24,interval_minutes=15,next_publish_at=? WHERE id=1",((now-timedelta(minutes=1)).isoformat(),))
    return now


def approved_post(now,**changes):
    text=COPY+'\n\nИсточник: https://news.example/story'
    values=dict(text=text,ai_status='approved',ai_score=91,ai_reason='Конкретная новость',ai_text_hash=editorial.text_hash(text),reviewed_at=now.isoformat())
    values.update(changes)
    return add_post(**values)


def test_structured_editor_approves_and_rejects(client,monkeypatch):
    mock_http(monkeypatch,lambda request:httpx.Response(200,json=verdict_response()))
    pid=add_post(source_summary='Разработчики выпустили новую модель.')
    response=client.post(f'/api/posts/{pid}/rewrite')
    assert response.status_code==200
    post=client.get('/api/posts').json()[0]
    assert post['ai_status']=='approved' and post['ai_score']==90
    assert post['ai_text_hash']==editorial.text_hash(post['text'])
    assert post['text'].startswith('Компания') and len(post['text'].split('\n\nИсточник:')[0])<=750
    mock_http(monkeypatch,lambda request:httpx.Response(200,json=verdict_response(False,15,reason='Учебная статья без новостного повода')))
    assert client.post(f'/api/posts/{pid}/rewrite').json()['ai_status']=='rejected'
    assert client.get('/api/events').json()[0]['kind']=='rejected'


@pytest.mark.parametrize('payload',[
    {'publishable':'yes','score':95,'reason':'OK text','text':COPY},
    {'publishable':True,'score':101,'reason':'OK text','text':COPY},
    {'publishable':True,'score':True,'reason':'OK text','text':COPY},
    {'publishable':True,'reason':'OK text','text':COPY},
])
def test_malformed_verdict_never_approves(client,monkeypatch,payload):
    mock_http(monkeypatch,lambda request:httpx.Response(200,json={'response':json.dumps(payload)}))
    pid=add_post()
    assert client.post(f'/api/posts/{pid}/rewrite').status_code==503
    assert client.get('/api/posts').json()[0]['ai_status']=='pending'


def test_enable_requires_successful_manual_send_to_same_chat(client,monkeypatch):
    settings={'name':'NeuroPulse','enabled':True,'chat_id':'-123','autopublish':True}
    assert client.put('/api/channels/1',json=settings).status_code==400
    settings['autopublish']=False
    client.put('/api/channels/1',json=settings)
    mock_http(monkeypatch,lambda request:httpx.Response(200,json={'message':{'body':{'mid':'test-manual'}}}))
    pid=add_post()
    assert client.post(f'/api/posts/{pid}/publish').status_code==200
    settings['autopublish']=True
    assert client.put('/api/channels/1',json=settings).status_code==200
    channel=client.get('/api/channels').json()[0]
    assert channel['autopublish']==1 and channel['verified_chat_id']=='-123'
    assert datetime.fromisoformat(channel['next_publish_at'])>datetime.now(timezone.utc)
    settings['chat_id']='-456'
    assert client.put('/api/channels/1',json=settings).status_code==400
    settings['autopublish']=False
    assert client.put('/api/channels/1',json=settings).status_code==200
    assert client.get('/api/channels').json()[0]['verified_chat_id']==''


def test_automatic_publish_sends_only_approved_text_once(client,monkeypatch):
    now=configure_auto(monkeypatch)
    pid=approved_post(now,image_url='https://news.example/image.jpg',image_approved=0)
    calls=[]
    def handler(request):
        calls.append(request)
        return httpx.Response(200,json={'message':{'body':{'mid':'auto-1'}}})
    mock_http(monkeypatch,handler)
    asyncio.run(module.automation_tick())
    asyncio.run(module.automation_tick())
    assert len(calls)==1
    assert 'attachments' not in json.loads(calls[0].content)
    post=client.get('/api/posts').json()[0]
    assert post['status']=='published' and post['publish_mode']=='auto'
    assert post['remote_id']=='auto-1'
    assert client.get('/api/events').json()[0]['kind']=='published'


@pytest.mark.parametrize('change',[
    {'ai_status':'pending'}, {'ai_status':'rejected'}, {'ai_score':60},
    {'ai_text_hash':'stale'}, {'reviewed_at':None}, {'error':'Previous error'},
    {'reviewed_at':'2020-01-01T00:00:00+00:00'},
    {'source_published_at':'2020-01-01T00:00:00+00:00'},
    {'created_at':'2020-01-01 00:00:00'},
])
def test_autopost_skips_unfit_candidates(client,monkeypatch,change):
    now=configure_auto(monkeypatch)
    approved_post(now,**change)
    def handler(request): pytest.fail('Unfit post must never be sent')
    mock_http(monkeypatch,handler)
    asyncio.run(module.automation_tick())
    assert client.get('/api/posts').json()[0]['status']=='draft'


@pytest.mark.parametrize('block',['off','channel_disabled','unverified','not_due','outside_hours','daily_limit','interval','duplicate'])
def test_schedule_constraints(client,monkeypatch,block):
    now=configure_auto(monkeypatch)
    approved_post(now)
    with module.db() as con:
        if block=='off': con.execute('UPDATE channels SET autopublish=0 WHERE id=1')
        if block=='channel_disabled': con.execute('UPDATE channels SET enabled=0 WHERE id=1')
        if block=='unverified': con.execute("UPDATE channels SET verified_chat_id='' WHERE id=1")
        if block=='not_due': con.execute('UPDATE channels SET next_publish_at=? WHERE id=1',((now+timedelta(hours=1)).isoformat(),))
        if block=='outside_hours':
            local_hour=now.astimezone(module.ZoneInfo('Europe/Moscow')).hour
            start=(local_hour+1)%24
            con.execute('UPDATE channels SET start_hour=?,end_hour=? WHERE id=1',(start,start+1))
        if block=='daily_limit': con.execute('UPDATE channels SET daily_limit=1 WHERE id=1')
    if block in ('daily_limit','interval','duplicate'):
        add_post(status='published',fingerprint='already-published',title='Другая новость' if block!='duplicate' else 'Новая модель ИИ',url='https://news.example/other',published_at=(now-timedelta(minutes=60 if block=='duplicate' else 1)).isoformat())
    mock_http(monkeypatch,lambda request:pytest.fail('Schedule should block network send'))
    asyncio.run(module.automation_tick())
    assert any(p['status']=='draft' for p in client.get('/api/posts').json())


@pytest.mark.parametrize('failure',['timeout','500','400','missing_mid'])
def test_publish_failures_pause_and_never_blindly_retry(client,monkeypatch,failure):
    now=configure_auto(monkeypatch)
    pid=approved_post(now)
    calls=[]
    def handler(request):
        calls.append(request)
        if failure=='timeout': raise httpx.ReadTimeout('test',request=request)
        if failure=='missing_mid': return httpx.Response(200,json={})
        return httpx.Response(int(failure),json={'error':'test'})
    mock_http(monkeypatch,handler)
    asyncio.run(module.automation_tick()); asyncio.run(module.automation_tick())
    assert len(calls)==1
    channel=client.get('/api/channels').json()[0]
    assert channel['autopublish']==0 and channel['automation_error']
    post=client.get('/api/posts').json()[0]
    assert post['status']==('draft' if failure=='400' else 'uncertain')
    if failure!='400':
        assert client.post(f'/api/posts/{pid}/publish').status_code==400
        assert client.post(f'/api/posts/{pid}/resolve',json={'published':False}).status_code==200
        assert client.get('/api/posts').json()[0]['status']=='draft'


def test_edit_and_channel_brief_invalidate_approval(client,monkeypatch):
    now=configure_auto(monkeypatch)
    pid=approved_post(now)
    assert client.put(f'/api/posts/{pid}',json={'text':COPY+' изменено'}).status_code==200
    assert client.get('/api/posts').json()[0]['ai_status']=='pending'
    with module.db() as con: con.execute("UPDATE posts SET ai_status='approved' WHERE id=?",(pid,))
    client.put('/api/channels/1',json={'name':'NeuroPulse','enabled':True,'editorial_brief':'Только новости робототехники'})
    assert client.get('/api/posts').json()[0]['ai_status']=='pending'


def test_pause_all_channels(client,monkeypatch):
    now=configure_auto(monkeypatch)
    approved_post(now)
    assert client.post('/api/automation/pause').status_code==200
    mock_http(monkeypatch,lambda request:pytest.fail('Paused automation must not send'))
    asyncio.run(module.automation_tick())
    assert client.get('/api/overview').json()['automatic_channels']==0


def test_interrupted_send_migration_never_retries(client):
    pid=add_post(status='publishing')
    module.init()
    assert client.get('/api/posts').json()[0]['status']=='uncertain'
    assert client.post(f'/api/posts/{pid}/publish').status_code==400


def test_rejected_news_saved_once_and_never_generated_twice(client,monkeypatch):
    rss='<rss version="2.0"><channel><title>T</title><item><title>Курс по нейросетям</title><link>https://news.example/course</link><description>Заплатите за обучение</description></item></channel></rss>'
    calls=[]
    def handler(request):
        if request.url.path=='/api/generate':
            calls.append(request)
            return httpx.Response(200,json=verdict_response(False,10,reason='Рекламный курс, а не новость'))
        return httpx.Response(200,text=rss)
    mock_http(monkeypatch,handler)
    client.post('/api/sources',json={'channel_id':1,'url':'https://news.example/rss'})
    asyncio.run(module.run_collection()); asyncio.run(module.run_collection())
    assert len(calls)==1
    assert module.COLLECT_STATE['duplicates']==1
    assert client.get('/api/posts').json()[0]['ai_status']=='rejected'

def test_daily_limit_resets_in_channel_timezone(client,monkeypatch):
    now=configure_auto(monkeypatch).replace(hour=21,minute=5)
    monkeypatch.setattr(module,'utcnow',lambda:now)
    with module.db() as con:
        con.execute('UPDATE channels SET daily_limit=1,next_publish_at=? WHERE id=1',((now-timedelta(minutes=1)).isoformat(),))
    # 23:30 yesterday in Moscow; same UTC calendar date must not consume today's limit.
    add_post(status='published',fingerprint='yesterday',title='Старая другая новость',url='https://news.example/old',published_at=now.replace(hour=20,minute=30).isoformat())
    approved_post(now)
    calls=[]
    def handler(request):
        calls.append(request)
        return httpx.Response(200,json={'message':{'body':{'mid':'new-day'}}})
    mock_http(monkeypatch,handler)
    asyncio.run(module.automation_tick())
    assert len(calls)==1


def test_image_approval_preserves_text_verdict(client,monkeypatch):
    now=configure_auto(monkeypatch)
    pid=approved_post(now,image_url='https://news.example/image.jpg')
    before=client.get('/api/posts').json()[0]
    assert client.put(f'/api/posts/{pid}',json={'text':before['text'],'image_url':before['image_url'],'image_approved':True}).status_code==200
    after=client.get('/api/posts').json()[0]
    assert after['ai_status']=='approved' and after['ai_text_hash']==before['ai_text_hash']
    calls=[]
    def handler(request):
        calls.append(json.loads(request.content))
        return httpx.Response(200,json={'message':{'body':{'mid':'image-auto'}}})
    mock_http(monkeypatch,handler)
    asyncio.run(module.automation_tick())
    assert calls[0]['attachments'][0]['payload']['url']==before['image_url']


def test_concurrent_send_claim_prevents_duplicate(client,monkeypatch):
    client.put('/api/channels/1',json={'name':'NeuroPulse','enabled':True,'chat_id':'-123'})
    pid=add_post()
    calls=[]
    async def handler(request):
        calls.append(request)
        await asyncio.sleep(.02)
        return httpx.Response(200,json={'message':{'body':{'mid':'once'}}})
    mock_http(monkeypatch,handler)
    async def run():
        return await asyncio.gather(module.publish(pid),module.publish(pid),return_exceptions=True)
    results=asyncio.run(run())
    assert len(calls)==1
    assert len([r for r in results if isinstance(r,ValueError)])==1


def test_review_batch_evaluates_legacy_drafts(client,monkeypatch):
    add_post(source_summary='Новая модель представлена разработчиками.')
    mock_http(monkeypatch,lambda request:httpx.Response(200,json=verdict_response()))
    assert client.post('/api/review').status_code==200
    for _ in range(200):
        status=client.get('/api/review/status').json()
        if not status['running']: break
        time.sleep(.005)
    assert status['done']==1 and not status['error']
    assert client.get('/api/posts').json()[0]['ai_status']=='approved'
