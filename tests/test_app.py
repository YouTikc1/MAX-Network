import asyncio
import base64
import json
import sqlite3
import time
from datetime import datetime,timezone,timedelta
from email.utils import format_datetime

import httpx
import pytest
from fastapi.testclient import TestClient
import app as module
import editorial

CLIENT=httpx.AsyncClient
SOURCE='Компания выпустила новую модель искусственного интеллекта. Разработчики открыли доступ к системе обработки документов и изображений и опубликовали результаты испытаний.'
COPY='Компания выпустила новую модель ИИ\n\nСистема обрабатывает документы и изображения. Разработчики открыли доступ к модели и опубликовали результаты испытаний. Обновление расширяет возможности работы с материалами.'
AUTH={'Authorization':'Basic '+base64.b64encode(b'admin:test-password').decode()}
BOOTSTRAP=module.ensure_news_source
QUEUE_REVIEW=module.queue_review

@pytest.fixture
def client(tmp_path,monkeypatch):
    monkeypatch.setattr(module,'DB',tmp_path/'test.db')
    monkeypatch.setattr(module,'PASSWORD','test-password')
    monkeypatch.setattr(module,'TOKEN','test-token')
    monkeypatch.setattr(module,'BACKGROUND_ENABLED',False)
    monkeypatch.setattr(module,'ensure_news_source',lambda cid:None)
    monkeypatch.setattr(module,'queue_review',lambda:None)
    for name in ['COLLECT_LOCK','PUBLISH_LOCK']:
        monkeypatch.setattr(module,name,asyncio.Lock())
    monkeypatch.setattr(editorial,'INFERENCE_LOCK',asyncio.Lock())
    for name in ['COLLECT_TASK','REVIEW_TASK']:
        monkeypatch.setattr(module,name,None)
    monkeypatch.setattr(module,'ONE_JOBS',{})
    monkeypatch.setattr(module,'ONE_TASKS',{})
    module.COLLECT_STATE.update(running=False,errors=[],finished=None)
    module.REVIEW_STATE.update(running=False,done=0,total=0,error=None)
    with TestClient(module.app,headers=AUTH) as c: yield c


def post(**changes):
    now=datetime.now(timezone.utc)
    values=dict(channel_id=1,title='Компания выпустила новую модель ИИ',text=COPY,url='https://news.example/story',fingerprint='story',
        source_summary=SOURCE,source_published_at=now.isoformat(),reviewed_at=now.isoformat(),ai_status='approved',ai_score=91,
        ai_reason='Свежий релиз модели',ai_text_hash=editorial.text_hash(COPY),editorial_revision=editorial.NEWS_REVISION,
        news_event='Компания выпустила новую модель ИИ',news_evidence=SOURCE[:60])
    values.update(changes)
    with module.db() as con:
        cur=con.execute('INSERT INTO posts('+','.join(values)+') VALUES ('+','.join('?' for _ in values)+')',list(values.values()))
        return cur.lastrowid


def network(monkeypatch,*,approve=True,score=91,copy=COPY,evidence=None,news=True,failure=None,rss=None,copy_valid=True):
    calls=[]
    def handler(request):
        calls.append(request)
        if request.url.path=='/api/generate':
            payload=json.loads(request.content);stage=payload['format']['title']
            if failure=='ollama': raise httpx.ConnectError('offline',request=request)
            if stage=='NewsDecision':
                body=json.loads(payload['prompt'])['article']['body']
                result={'publishable':approve,'is_news':news,'score':score,'reason':'Свежий релиз модели' if approve else 'Это инструкция, а не новость','event':'Компания выпустила новую модель ИИ' if approve else '', 'evidence':(body[:60] if evidence is None else evidence) if approve else ''}
            elif stage=='CopyCheck': result={'valid':copy_valid,'reason':'Факты подтверждены материалом' if copy_valid else 'В тексте есть неподтверждённые характеристики'}
            else: result={'text':copy}
            return httpx.Response(200,json={'response':json.dumps(result,ensure_ascii=False)})
        if request.url.host=='news.example': return httpx.Response(200,text=rss or '<rss version="2.0"><channel><title>Empty</title></channel></rss>')
        if failure=='timeout': raise httpx.ReadTimeout('timeout',request=request)
        if failure in ('400','500'): return httpx.Response(int(failure),json={})
        if failure=='missing_mid': return httpx.Response(200,json={})
        return httpx.Response(200,json={'message':{'body':{'mid':'remote-message'}}})
    monkeypatch.setattr(module.httpx,'AsyncClient',lambda *a,**kw:CLIENT(transport=httpx.MockTransport(handler),**kw))
    return calls


def configure(client,monkeypatch,automatic=False):
    client.put('/api/channels/1',json={'name':'NeuroPulse','enabled':automatic,'chat_id':'-123'})
    now=datetime.now(timezone.utc)
    if automatic:
        with module.db() as con:
            con.execute("UPDATE channels SET autopublish=1,verified_chat_id=chat_id,start_hour=0,end_hour=24,interval_minutes=15,next_publish_at=? WHERE id=1",((now-timedelta(minutes=1)).isoformat(),))
    monkeypatch.setattr(module,'utcnow',lambda:now)
    return now


def wait_job(client,path='/api/publish-jobs',cid='1'):
    for _ in range(300):
        data=client.get(path).json()
        result=data[cid] if cid else data
        if not result['running']: return result
        time.sleep(.005)
    pytest.fail('Background job did not complete')


def test_auth_ui_and_sources_crud(client):
    assert client.get('/api/sources',headers={'Authorization':''}).status_code==401
    for path in ['/','/static/app.js','/static/app.css','/api/overview','/api/events']:
        assert client.get(path).status_code==200
    cid=client.post('/api/channels',json={'name':'New','chat_id':'-456'}).json()['id']
    payload={'channel_id':cid,'url':'https://news.example/rss'}
    assert client.post('/api/sources',json=payload).status_code==200
    assert client.post('/api/sources',json=payload).status_code==409
    sid=client.get('/api/sources').json()[0]['id']
    assert client.delete(f'/api/channels/{cid}').status_code==409
    assert client.delete(f'/api/sources/{sid}').status_code==200
    assert client.delete(f'/api/channels/{cid}').status_code==200

@pytest.mark.parametrize('url',['https://127.0.0.1/rss','https://10.0.0.1/rss','https://example.com:bad/rss','https://u:p@example.com/rss','javascript:alert(1)'])
def test_source_validation(client,url):
    assert client.post('/api/sources',json={'channel_id':1,'url':url}).status_code==400


def test_two_stage_news_and_no_source_footer(client,monkeypatch):
    calls=network(monkeypatch)
    result=asyncio.run(editorial.review('Новая модель',SOURCE,editorial.DEFAULT_EDITORIAL_BRIEF))
    assert result.publishable and result.text==COPY
    assert 'Источник:' not in result.text
    assert [json.loads(r.content)['format']['title'] for r in calls]==['NewsDecision','PostCopy','CopyCheck']

@pytest.mark.parametrize('copy',[
    'Пайпы OpenWebUI: короткий заголовок и 1–2 абзаца, 100–750 символов, на русском от третьего лица. '+COPY,
    'В статье автор рассказывает о новой модели. '+COPY,
    'Статья предоставляет полезные данные. '+COPY,
    'Меня зовут Дмитрий. '+COPY,
    '### Заголовок\n'+COPY,
    COPY+'\nИсточник: https://news.example/story',
    'Only English text. '*20,
    COPY*5,
])
def test_instruction_leak_and_article_recaps_never_approved(client,monkeypatch,copy):
    calls=network(monkeypatch,copy=copy)
    with pytest.raises(editorial.EditorialUnavailable):
        asyncio.run(editorial.review('Release',SOURCE,'ИИ'))
    assert len(calls)<=3

@pytest.mark.parametrize('news,approve',[(False,True),(False,False),(True,False)])
def test_non_news_not_written(client,monkeypatch,news,approve):
    calls=network(monkeypatch,news=news,approve=approve)
    verdict=asyncio.run(editorial.review('Как настроить OpenWebUI',SOURCE,'ИИ'))
    assert not verdict.publishable and verdict.text=='' and verdict.score<=74
    assert len(calls)==1


def test_unverified_evidence_blocks_writer(client,monkeypatch):
    calls=network(monkeypatch,evidence='Компания объявила совершенно несуществующее событие')
    with pytest.raises(editorial.EditorialUnavailable):asyncio.run(editorial.review('Модель',SOURCE,'ИИ'))
    assert len(calls)==1


def test_unavailable_model_no_publication(client,monkeypatch):
    calls=network(monkeypatch,failure='ollama')
    pid=post(ai_status='pending',editorial_revision=0)
    configure(client,monkeypatch)
    assert client.post(f'/api/posts/{pid}/publish').status_code==503
    assert not any(r.url.host=='platform-api.max.ru' for r in calls)


def test_collection_auto_review_and_duplicate_protection(client,monkeypatch):
    date=format_datetime(datetime.now(timezone.utc))
    rss=f'<rss version="2.0"><channel><title>N</title><item><title>Компания выпустила новую модель ИИ</title><link>https://news.example/story?utm_source=rss</link><pubDate>{date}</pubDate><description><![CDATA[{SOURCE}<img src="/photo.jpg">]]></description></item></channel></rss>'
    calls=network(monkeypatch,rss=rss)
    client.post('/api/sources',json={'channel_id':1,'url':'https://news.example/rss'})
    for _ in range(2):
        client.post('/api/collect')
        wait_job(client,'/api/collect/status',None)
    posts=client.get('/api/posts').json()
    assert len(posts)==1 and posts[0]['ai_status']=='approved'
    assert posts[0]['text']==COPY and posts[0]['image_url']=='https://news.example/photo.jpg'
    assert posts[0]['source_published_at']
    assert len([r for r in calls if r.url.path=='/api/generate'])==3
    assert module.COLLECT_STATE['duplicates']==1
    client.delete('/api/posts/'+str(posts[0]['id']))
    asyncio.run(module.run_collection())
    assert client.get('/api/posts').json()==[] and module.COLLECT_STATE['duplicates']==1


def test_publish_one_works_without_channel_enable_or_photo_checkbox(client,monkeypatch):
    configure(client,monkeypatch)
    calls=network(monkeypatch)
    post(image_url='https://news.example/photo.jpg',image_approved=0)
    assert client.post('/api/channels/1/publish-one').status_code==202
    result=wait_job(client)
    assert result['post_id'] and not result['error']
    sends=[r for r in calls if r.url.host=='platform-api.max.ru']
    assert len(sends)==1
    body=json.loads(sends[0].content)
    assert body['text']==COPY and 'Источник' not in body['text']
    assert body['attachments'][0]['payload']['url']=='https://news.example/photo.jpg'


def test_publish_one_selects_freshest_before_highest_score(client,monkeypatch):
    now=configure(client,monkeypatch)
    calls=network(monkeypatch)
    post(fingerprint='older',source_published_at=(now-timedelta(hours=8)).isoformat(),ai_score=99)
    newer=post(fingerprint='newer',url='https://news.example/newer',title='Вторая компания открыла доступ к системе',source_published_at=(now-timedelta(hours=1)).isoformat(),ai_score=80)
    client.post('/api/channels/1/publish-one')
    assert wait_job(client)['post_id']==newer
    assert len([r for r in calls if r.url.host=='platform-api.max.ru'])==1


def test_publish_one_prepares_new_news_without_extra_buttons(client,monkeypatch):
    configure(client,monkeypatch)
    date=format_datetime(datetime.now(timezone.utc))
    rss=f'<rss version="2.0"><channel><title>N</title><item><title>Компания выпустила модель</title><link>https://news.example/story</link><pubDate>{date}</pubDate><description>{SOURCE}</description></item></channel></rss>'
    calls=network(monkeypatch,rss=rss)
    client.post('/api/sources',json={'channel_id':1,'url':'https://news.example/rss'})
    client.post('/api/channels/1/publish-one')
    result=wait_job(client)
    assert result['post_id'] and not result['error']
    assert client.get('/api/posts').json()[0]['status']=='published'


def test_empty_news_never_publishes_filler(client,monkeypatch):
    configure(client,monkeypatch)
    calls=network(monkeypatch)
    client.post('/api/channels/1/publish-one')
    result=wait_job(client)
    assert result['error'] and not result['post_id']
    assert not any(r.url.host=='platform-api.max.ru' for r in calls)

@pytest.mark.parametrize('changes',[
    {'editorial_revision':0},{'ai_status':'pending'},{'ai_status':'rejected'},{'ai_score':60},
    {'ai_text_hash':'changed'},{'news_event':''},{'news_evidence':''},{'source_published_at':None},
    {'source_published_at':'2020-01-01T00:00:00+00:00'},{'reviewed_at':'2020-01-01T00:00:00+00:00'},
    {'text':'короткий заголовок и 1–2 абзаца, 100–750 символов, на русском от третьего лица. '+COPY},
])
def test_automation_never_sends_unfit_candidates(client,monkeypatch,changes):
    configure(client,monkeypatch,True)
    calls=network(monkeypatch)
    post(**changes)
    asyncio.run(module.automation_tick())
    assert not any(r.url.host=='platform-api.max.ru' for r in calls)

@pytest.mark.parametrize('constraint',['paused','disabled','unverified','future','hours','limit','interval','duplicate'])
def test_schedule_constraints(client,monkeypatch,constraint):
    now=configure(client,monkeypatch,True);post()
    with module.db() as con:
        if constraint=='paused':con.execute('UPDATE channels SET autopublish=0')
        if constraint=='disabled':con.execute('UPDATE channels SET enabled=0')
        if constraint=='unverified':con.execute("UPDATE channels SET verified_chat_id=''")
        if constraint=='future':con.execute('UPDATE channels SET next_publish_at=?',((now+timedelta(hours=1)).isoformat(),))
        if constraint=='hours':
            start=(now.astimezone(module.ZoneInfo('Europe/Moscow')).hour+1)%24
            con.execute('UPDATE channels SET start_hour=?,end_hour=?',(start,start+1))
        if constraint=='limit':con.execute('UPDATE channels SET daily_limit=1')
    if constraint in ('limit','interval','duplicate'):
        post(fingerprint='sent',url='https://news.example/other',title='Компания выпустила новую модель ИИ' if constraint=='duplicate' else 'Иная новость',status='published',published_at=(now-timedelta(minutes=60 if constraint=='duplicate' else 1)).isoformat())
    calls=network(monkeypatch);asyncio.run(module.automation_tick())
    assert not any(r.url.host=='platform-api.max.ru' for r in calls)

@pytest.mark.parametrize('failure',['timeout','400','500','missing_mid'])
def test_publish_error_durable_pause_and_no_retry(client,monkeypatch,failure):
    configure(client,monkeypatch,True);pid=post();calls=network(monkeypatch,failure=failure)
    asyncio.run(module.automation_tick());asyncio.run(module.automation_tick())
    assert len([r for r in calls if r.url.host=='platform-api.max.ru'])==1
    assert client.get('/api/channels').json()[0]['autopublish']==0
    status=client.get('/api/posts').json()[0]['status']
    assert status==('draft' if failure=='400' else 'uncertain')
    if status=='uncertain':
        assert client.post(f'/api/posts/{pid}/publish').status_code==400
        assert client.post(f'/api/posts/{pid}/resolve',json={'published':False}).status_code==200


def test_edit_is_saved_and_rechecked_automatically(client,monkeypatch):
    network(monkeypatch);pid=post()
    monkeypatch.setattr(module,'queue_review',QUEUE_REVIEW)
    edited=COPY.replace('Обновление расширяет','Новая система расширяет')
    assert client.put(f'/api/posts/{pid}',json={'text':edited,'image_url':''}).status_code==200
    wait_job(client,'/api/review/status',None)
    saved=client.get('/api/posts').json()[0]
    assert saved['text']==edited and saved['ai_status']=='approved'
    assert saved['ai_text_hash']==editorial.text_hash(edited)


def test_direct_publish_rechecks_bad_legacy_text(client,monkeypatch):
    configure(client,monkeypatch)
    pid=post(text='короткий заголовок и 1–2 абзаца, 100–750 символов, на русском от третьего лица. '+COPY,editorial_revision=0)
    calls=network(monkeypatch)
    assert client.post(f'/api/posts/{pid}/publish').status_code==200
    sent=[r for r in calls if r.url.host=='platform-api.max.ru']
    assert json.loads(sent[0].content)['text']==COPY


def test_concurrent_send_is_once(client,monkeypatch):
    configure(client,monkeypatch);pid=post();calls=network(monkeypatch)
    async def run():return await asyncio.gather(module.publish(pid),module.publish(pid),return_exceptions=True)
    result=asyncio.run(run())
    assert len([r for r in calls if r.url.host=='platform-api.max.ru'])==1
    assert any(isinstance(r,ValueError) for r in result)


def test_enable_schedule_requires_successful_send(client,monkeypatch):
    settings={'name':'NeuroPulse','enabled':True,'chat_id':'-123','autopublish':True}
    assert client.put('/api/channels/1',json=settings).status_code==400
    configure(client,monkeypatch);pid=post();network(monkeypatch)
    assert client.post(f'/api/posts/{pid}/publish').status_code==200
    assert client.put('/api/channels/1',json=settings).status_code==200
    settings['chat_id']='-456'
    assert client.put('/api/channels/1',json=settings).status_code==400


def test_recovery_and_revision_migration_preserve_records(client):
    pid=post(status='publishing',editorial_revision=0)
    other=post(fingerprint='draft',editorial_revision=0)
    with module.db() as con:con.execute('UPDATE channels SET autopublish=1')
    module.init();module.init()
    posts={p['id']:p for p in client.get('/api/posts').json()}
    assert posts[pid]['status']=='uncertain'
    assert posts[other]['status']=='draft' and posts[other]['ai_status']=='pending'
    assert posts[other]['text']==COPY
    assert client.get('/api/channels').json()[0]['autopublish']==0


def test_source_date_backfill_and_news_feed_preserve_data(client,monkeypatch):
    now=datetime.now(timezone.utc)
    pid=post(source_published_at=None,source_summary='',editorial_revision=0)
    rss=f'<rss version="2.0"><channel><title>N</title><item><title>Модель</title><link>https://news.example/story</link><pubDate>{format_datetime(now)}</pubDate><description>{SOURCE}</description></item></channel></rss>'
    network(monkeypatch,rss=rss)
    client.post('/api/sources',json={'channel_id':1,'url':'https://news.example/rss'})
    asyncio.run(module.run_collection())
    saved=client.get('/api/posts').json()[0]
    assert saved['id']==pid and saved['text']==COPY and saved['source_published_at']
    BOOTSTRAP(1);BOOTSTRAP(1)
    sources=client.get('/api/sources').json()
    assert len(sources)==2 and any('/rss/news/' in r['url'] for r in sources)


def test_scope_recheck_respects_channel(client,monkeypatch):
    first=post(ai_status='rejected');second=post(channel_id=2,fingerprint='second',ai_status='rejected')
    network(monkeypatch)
    client.post('/api/review',json={'include_rejected':True,'channel_id':1})
    wait_job(client,'/api/review/status',None)
    posts={p['id']:p for p in client.get('/api/posts').json()}
    assert posts[first]['ai_status']=='approved' and posts[second]['ai_status']=='rejected'

def test_old_schema_migration_preserves_actual_records(client,tmp_path,monkeypatch):
    old=tmp_path/'old.db'
    with sqlite3.connect(old) as con:
        con.executescript('''CREATE TABLE channels(id INTEGER PRIMARY KEY,name TEXT NOT NULL,chat_id TEXT,enabled INTEGER DEFAULT 0,autopublish INTEGER DEFAULT 0);
        CREATE TABLE sources(id INTEGER PRIMARY KEY,channel_id INTEGER NOT NULL,url TEXT NOT NULL,UNIQUE(channel_id,url));
        CREATE TABLE posts(id INTEGER PRIMARY KEY,channel_id INTEGER NOT NULL,title TEXT,text TEXT NOT NULL,url TEXT,fingerprint TEXT UNIQUE,status TEXT NOT NULL DEFAULT 'draft',created_at TEXT DEFAULT CURRENT_TIMESTAMP,published_at TEXT,error TEXT,remote_id TEXT);
        INSERT INTO channels(id,name,chat_id) VALUES (7,'Мой канал','-777');
        INSERT INTO sources(channel_id,url) VALUES (7,'https://news.example/rss');
        INSERT INTO posts(channel_id,title,text,fingerprint) VALUES (7,'Старый заголовок','Мой сохранённый текст','old-story');''')
    monkeypatch.setattr(module,'DB',old)
    module.init();module.init()
    with module.db() as con:
        assert con.execute('SELECT COUNT(*) FROM channels').fetchone()[0]==1
        assert con.execute('SELECT name FROM channels').fetchone()[0]=='Мой канал'
        assert con.execute('SELECT COUNT(*) FROM sources').fetchone()[0]==1
        row=con.execute('SELECT * FROM posts').fetchone()
        assert row['text']=='Мой сохранённый текст' and row['ai_status']=='pending'


def test_double_click_during_job_does_not_send_second_post(client,monkeypatch):
    configure(client,monkeypatch);post();calls=network(monkeypatch)
    original=module.publish
    async def slow_publish(*a,**kw):
        await asyncio.sleep(.05)
        return await original(*a,**kw)
    monkeypatch.setattr(module,'publish',slow_publish)
    assert client.post('/api/channels/1/publish-one').status_code==202
    assert client.post('/api/channels/1/publish-one').status_code==409
    assert not wait_job(client)['error']
    assert len([r for r in calls if r.url.host=='platform-api.max.ru'])==1


def test_generated_copy_retry_never_releases_prompt_leak(client,monkeypatch):
    calls=network(monkeypatch,copy='Пайпы OpenWebUI: короткий заголовок и 1–2 абзаца, 100–750 символов, на русском от третьего лица. '+COPY)
    configure(client,monkeypatch)
    pid=post(ai_status='pending',editorial_revision=0)
    assert client.post(f'/api/posts/{pid}/publish').status_code==503
    assert len([r for r in calls if r.url.path=='/api/generate'])==3
    assert not any(r.url.host=='platform-api.max.ru' for r in calls)


def test_unsupported_copy_claims_are_not_published(client,monkeypatch):
    configure(client,monkeypatch)
    pid=post(ai_status='pending',editorial_revision=0)
    calls=network(monkeypatch,copy_valid=False)
    assert client.post(f'/api/posts/{pid}/publish').status_code==503
    assert not any(r.url.host=='platform-api.max.ru' for r in calls)
