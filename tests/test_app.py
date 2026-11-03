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
        news_event='Компания выпустила новую модель ИИ',news_evidence=SOURCE[:60],headline=COPY.split('\n\n')[0])
    values.update(changes)
    with module.db() as con:
        cur=con.execute('INSERT INTO posts('+','.join(values)+') VALUES ('+','.join('?' for _ in values)+')',list(values.values()))
        return cur.lastrowid


def network(monkeypatch,*,approve=True,score=91,copy=COPY,evidence=None,news=True,failure=None,rss=None,copy_valid=True,headline=None):
    calls=[]
    def handler(request):
        calls.append(request)
        if request.url.path=='/api/generate':
            payload=json.loads(request.content);stage=payload['format']['title']
            if failure=='ollama': raise httpx.ConnectError('offline',request=request)
            if stage=='NewsDecision':
                body=json.loads(payload['prompt'])['article']['body']
                result={'publishable':approve,'is_news':news,'score':score,'reason':'Свежий релиз модели' if approve else 'Это инструкция, а не новость','event':json.loads(payload['prompt'])['article']['title'] if approve else '', 'is_ai':True, 'support_ids':([0,1] if evidence is None else [999]) if approve else []}
            elif stage=='CopyCheck': result={'valid':copy_valid,'reason':'Факты подтверждены материалом' if copy_valid else 'В тексте есть неподтверждённые характеристики'}
            else:
                head=COPY.split('\n\n')[0] if headline is None and copy==COPY else (headline or '')
                result={'headline':head,'text':copy[len(head)+2:] if head and copy.startswith(head+'\n\n') else copy}
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


def test_invalid_fragment_ids_are_retried_without_writer(client,monkeypatch):
    calls=network(monkeypatch,evidence='Компания объявила совершенно несуществующее событие')
    with pytest.raises(editorial.EditorialUnavailable):asyncio.run(editorial.review('Модель',SOURCE,'ИИ'))
    assert len(calls)==2


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
    assert body['text'].startswith('<b>'+COPY.replace('\n\n','</b>\n\n',1)+'\n\n<a ') and 'Источник' not in body['text']
    assert body['format']=='html' and 'https://max.ru/channel_NeuroPulse' in body['text']
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
    sends=[r for r in calls if r.url.host=='platform-api.max.ru']
    assert len(sends)==(1 if constraint in ('future','hours','limit','interval') else 0)

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
    assert json.loads(sent[0].content)['text'].startswith('<b>'+COPY.replace('\n\n','</b>\n\n',1)+'\n\n<a ')


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
    assert len(sources)==1+len(module.NEWS_FEEDS) and any('/rss/news/' in r['url'] for r in sources)


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
        assert con.execute('SELECT COUNT(*) FROM channels').fetchone()[0]==5
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

@pytest.mark.parametrize('title,body',[
    ('Microsoft прекращает поддержку Copilot на смарт-телевизорах LG','Microsoft сообщила о прекращении сервиса Copilot в 2026 году.'),
    ('Xiaomi выпустила открытую модель OCR-0 для распознавания документов','Модель распознаёт текст и извлекает поля документов.'),
    ('Apple приобрела ИИ-стартап Ascend.io','Компания заключила сделку по покупке разработчика ИИ-платформы.'),
    ('Anthropic переименовала подписки и сняла лимиты','Разработчик Claude обновил тарифы модели.'),
    ('Сэм Альтман высказался о локальном инференсе','Руководитель OpenAI сделал заявление об обработке данных.'),
])
def test_examples_from_user_are_ai_relevant(client,title,body):
    assert editorial.is_ai_story(title,body)


def test_nodpi_cannot_get_model_approval_or_be_sent(client,monkeypatch):
    title='Выпуск NoDPI4Android 2.0 — открытого инструмента для модификации трафика'
    body='Приложение переписано с нуля на Kotlin, Jetpack Compose и Material 3. Работает прокси-сервер.'
    calls=network(monkeypatch)
    result=asyncio.run(editorial.review(title,body,'Новости ИИ'))
    assert not result.publishable and result.score==0 and not calls
    configure(client,monkeypatch)
    pid=post(title=title,source_summary=body,ai_score=95)
    assert client.post(f'/api/posts/{pid}/publish').status_code==400
    assert not calls


def test_paraphrased_event_uses_supplied_fragments_not_exact_echo(client,monkeypatch):
    network(monkeypatch)
    result=asyncio.run(editorial.review('Apple купила ИИ-стартап',SOURCE,'ИИ'))
    assert result.publishable and result.evidence
    assert result.evidence in 'Apple купила ИИ-стартап '+SOURCE


def pool_feed(total=25):
    now=datetime.now(timezone.utc)
    items=[]
    for i in range(total):
        items.append(f'<item><title>Компания Product{i} выпустила модель ИИ версии {i}</title><link>https://news.example/product{i}</link><pubDate>{format_datetime(now-timedelta(minutes=i))}</pubDate><description>{SOURCE}</description></item>')
    return '<rss version="2.0"><channel><title>AI news</title>'+''.join(items)+'</channel></rss>'


def test_reserve_prepares_ten_plus_two_spares_from_more_than_ten_candidates(client,monkeypatch):
    network(monkeypatch,rss=pool_feed())
    client.post('/api/sources',json={'channel_id':1,'url':'https://news.example/rss'})
    asyncio.run(module.run_collection())
    ready=module.ready_posts(1)
    assert len(ready)==10
    assert len(module.prepared_posts(1))==12
    with module.db() as con:
        assert con.execute('SELECT COUNT(*) FROM posts').fetchone()[0]==25
        assert con.execute("SELECT COUNT(*) FROM posts WHERE ai_status='standby'").fetchone()[0]==2
    status=client.get('/api/queue/status').json()['channels'][0]
    assert status['ready']==10 and status['missing']==0
    assert client.get('/api/overview').json()['approved']==10
    assert len([p for p in client.get('/api/posts').json() if p['queue_ready']])==10


def test_consumed_post_is_immediately_replaced_by_spare(client,monkeypatch):
    configure(client,monkeypatch)
    network(monkeypatch,rss=pool_feed())
    client.post('/api/sources',json={'channel_id':1,'url':'https://news.example/rss'})
    asyncio.run(module.run_collection())
    first=module.best_post(1)
    asyncio.run(module.publish(first['id']))
    assert len(module.ready_posts(1))==10
    assert len(module.prepared_posts(1))==11
    asyncio.run(module.run_collection())
    assert len(module.prepared_posts(1))==12
    assert len(module.ready_posts(1))==10


def test_reserve_does_not_fill_with_irrelevant_or_low_quality_material(client,monkeypatch):
    feed=pool_feed(3)
    extra=''.join(f'<item><title>NoDPI Android app {i}</title><link>https://news.example/not-ai-{i}</link><pubDate>{format_datetime(datetime.now(timezone.utc))}</pubDate><description>Kotlin Jetpack Compose Material 3 proxy server</description></item>' for i in range(15))
    feed=feed.replace('</channel>',extra+'</channel>')
    calls=network(monkeypatch,rss=feed)
    client.post('/api/sources',json={'channel_id':1,'url':'https://news.example/rss'})
    asyncio.run(module.run_collection())
    assert len(module.ready_posts(1))==3
    status=client.get('/api/queue/status').json()['channels'][0]
    assert status['missing']==7
    assert len([r for r in calls if r.url.path=='/api/generate'])==9


def test_one_bad_generation_does_not_stop_next_good_news(client,monkeypatch):
    bad=post(title='OpenAI выпустила Product0',fingerprint='bad',ai_status='pending')
    good=post(title='Google выпустила Gemini Product1',fingerprint='good',ai_status='pending',source_published_at=(datetime.now(timezone.utc)-timedelta(minutes=1)).isoformat())
    network(monkeypatch)
    original=module.review
    async def review(title,*args,**kw):
        if 'Product0' in title: raise editorial.EditorialUnavailable('Проблема текста одной новости','quality')
        return await original(title,*args,**kw)
    monkeypatch.setattr(module,'review',review)
    asyncio.run(module.review_pending())
    posts={p['id']:p for p in client.get('/api/posts').json()}
    assert posts[bad]['review_after'] and posts[bad]['review_attempts']==1
    assert posts[good]['ai_status']=='approved'


def test_expired_news_leaves_ready_reserve(client,monkeypatch):
    post(source_published_at=(datetime.now(timezone.utc)-timedelta(hours=49)).isoformat())
    module.refresh_reserve()
    assert module.ready_posts(1)==[]
    assert client.get('/api/posts').json()[0]['ai_status']=='expired'


def test_primary_signature_is_correct_and_preserved_by_old_clients(client):
    channel=client.get('/api/channels').json()[0]
    assert channel['channel_url']=='https://max.ru/channel_NeuroPulse'
    assert channel['signature_text']=='NeuroPulse | Нейросети и ИИ'
    assert client.put('/api/channels/1',json={'name':'NeuroPulse','chat_id':'-123'}).status_code==200
    assert client.get('/api/channels').json()[0]['channel_url']==channel['channel_url']
    assert client.put('/api/channels/1',json={'name':'N','channel_url':'javascript:alert(1)'}).status_code==400


def long_copy(count):
    return '\n\n'.join(f'Испытание №{i}: система обработала {100+i} документов за {20+i} секунд. В наборе было {30+i} изображений; доступ открыли для {200+i} участников.' for i in range(count))


def test_new_post_longer_than_750_is_kept(client,monkeypatch):
    long_text=long_copy(8)
    network(monkeypatch,copy=long_text)
    result=asyncio.run(editorial.review('OpenAI выпустила модель',SOURCE,'ИИ'))
    assert len(result.text)>750 and result.text==long_text


def test_html_payloads_keep_all_characters_and_put_link_after_news():
    from publication import message_payloads,footer,units
    import html
    text=('Новая модель ИИ: <тест> & данные 😀. '+'Все характеристики и условия сохранены. '*12+'\n\n')*30
    signature=footer('https://max.ru/channel_neuropulse','NeuroPulse | Нейросети и ИИ')
    parts=message_payloads(text,'https://max.ru/channel_neuropulse','NeuroPulse | Нейросети и ИИ','https://news.example/photo.jpg')
    assert len(parts)>1
    assert all(p['format']=='html' and units(p['text'])<=4000 for p in parts)
    assert parts[-1]['text'].endswith(signature)
    assert all(signature not in p['text'] for p in parts[:-1])
    reconstructed=''.join(p['text'] if i<len(parts)-1 else p['text'][:-len(signature)] for i,p in enumerate(parts))
    assert html.unescape(reconstructed)==text
    assert 'attachments' in parts[0] and all('attachments' not in p for p in parts[1:])
    assert '&lt;тест&gt; &amp;' in reconstructed


def test_signature_text_is_html_escaped():
    from publication import message_payloads
    parts=message_payloads(COPY,'https://max.ru/channel_neuropulse','<NeuroPulse> & ИИ')
    assert '<b>&lt;NeuroPulse&gt; &amp; ИИ</b>' in parts[0]['text']
    assert '<NeuroPulse>' not in parts[0]['text']


def test_long_publish_sends_all_parts_and_records_ids(client,monkeypatch):
    from publication import message_payloads
    configure(client,monkeypatch)
    long_text=long_copy(40)
    pid=post(text=long_text,ai_text_hash=editorial.text_hash(long_text),image_url='https://news.example/photo.jpg')
    calls=[]
    def handler(request):
        calls.append(request)
        return httpx.Response(200,json={'message':{'body':{'mid':'part-'+str(len(calls))}}})
    monkeypatch.setattr(module.httpx,'AsyncClient',lambda *a,**kw:CLIENT(transport=httpx.MockTransport(handler),**kw))
    response=client.post(f'/api/posts/{pid}/publish')
    assert response.status_code==200
    assert response.json()['parts']==len(calls)>1
    saved=client.get('/api/posts').json()[0]
    assert saved['text']==long_text and saved['status']=='published'
    assert json.loads(saved['remote_ids'])==['part-'+str(i+1) for i in range(len(calls))]
    with module.db() as con:
        assert con.execute("SELECT COUNT(*) FROM post_deliveries WHERE post_id=? AND status='published'",(pid,)).fetchone()[0]==len(calls)
    assert client.post(f'/api/posts/{pid}/publish').status_code==400
    assert len(calls)==response.json()['parts']


def test_partial_long_post_failure_cannot_be_automatically_repeated(client,monkeypatch):
    configure(client,monkeypatch,True)
    long_text=long_copy(60)
    pid=post(text=long_text,ai_text_hash=editorial.text_hash(long_text))
    calls=[]
    def handler(request):
        calls.append(request)
        if len(calls)==2:return httpx.Response(400,json={})
        return httpx.Response(200,json={'message':{'body':{'mid':'first-part'}}})
    monkeypatch.setattr(module.httpx,'AsyncClient',lambda *a,**kw:CLIENT(transport=httpx.MockTransport(handler),**kw))
    asyncio.run(module.automation_tick());asyncio.run(module.automation_tick())
    saved=client.get('/api/posts').json()[0]
    assert saved['status']=='uncertain' and json.loads(saved['remote_ids'])==['first-part']
    assert 'Отправлено частей 1' in saved['error']
    assert client.get('/api/channels').json()[0]['autopublish']==0
    assert len(calls)==2
    assert client.post(f'/api/posts/{pid}/publish').status_code==400
    assert len(calls)==2


def test_generation_token_limit_retries_instead_of_publishing_cut_text(client):
    long_copy=COPY*10
    calls=[]
    def handler(request):
        calls.append(json.loads(request.content))
        return httpx.Response(200,json={'response':json.dumps({'headline':'','text':long_copy}),'done_reason':'length' if len(calls)==1 else 'stop'})
    async def run():
        async with CLIENT(transport=httpx.MockTransport(handler)) as c:
            return await editorial.generate(c,'write',{'source':SOURCE},editorial.PostCopy)
    result=asyncio.run(run())
    assert result.text==long_copy
    assert [r['options']['num_predict'] for r in calls]==[2048,4096]


def test_partial_retry_requires_explicit_removed_parts(client,monkeypatch):
    pid=post(status='uncertain',remote_ids=json.dumps(['already-published']))
    assert client.post(f'/api/posts/{pid}/resolve',json={'published':False}).status_code==409
    assert client.post(f'/api/posts/{pid}/resolve',json={'published':False,'parts_removed':True}).status_code==200


def test_topic_presets_links_and_repeatable_migration(client):
    before=client.get('/api/channels').json()
    assert {c['topic'] for c in before}=={'ai','tech','finance','sport','business'}
    for c in before:
        assert c['channel_url'].startswith('https://max.ru/channel_')
        assert not c['autopublish']
    module.init()
    assert client.get('/api/channels').json()==before


@pytest.mark.parametrize('topic,title',[
    ('finance','Банк снизил ключевую ставку'),
    ('sport','Сборная выиграла чемпионат по футболу'),
    ('business','Компания объявила сделку по поглощению'),
    ('tech','Выпущен новый смартфон'),
])
def test_non_ai_topics_review_and_publish(client,monkeypatch,topic,title):
    channel=next(c for c in client.get('/api/channels').json() if c['topic']==topic)
    payload={**channel,'chat_id':'-123'}
    assert client.put('/api/channels/'+str(channel['id']),json=payload).status_code==200
    calls=network(monkeypatch)
    verdict=asyncio.run(editorial.review(title,title+'. Подтверждены результаты события.',channel['editorial_brief'],topic=topic))
    assert verdict.publishable
    pid=post(channel_id=channel['id'],title=title,source_summary=title)
    assert client.post(f'/api/posts/{pid}/publish').status_code==200
    sent=json.loads(next(r for r in calls if r.url.host=='platform-api.max.ru').content)
    assert channel['channel_url'] in sent['text']
    assert sent['text'].startswith('<b>')


def test_wrong_topic_blocked_even_with_approved_copy(client,monkeypatch):
    channel=next(c for c in client.get('/api/channels').json() if c['topic']=='sport')
    client.put('/api/channels/'+str(channel['id']),json={**channel,'chat_id':'-123'})
    calls=network(monkeypatch)
    pid=post(channel_id=channel['id'])
    assert client.post(f'/api/posts/{pid}/publish').status_code==400
    assert not any(r.url.host=='platform-api.max.ru' for r in calls)


def test_headline_escaping_and_multiple_images():
    from publication import message_payloads
    parts=message_payloads('Новый <релиз> & тест\n\nТекст новости', 'https://max.ru/channel_TechPulse','TechPulse',
        'https://news.example/a.jpg',['https://news.example/a.jpg','https://news.example/b.jpg'])
    assert parts[0]['text'].startswith('<b>Новый &lt;релиз&gt; &amp; тест</b>\n\nТекст новости\n\n<a ')
    assert len(parts[0]['attachments'])==2


def test_rss_media_and_clear_images(client):
    images=module.feed_images({'link':'https://news.example/story','summary':'<img src="/a.jpg"><img src="/b.jpg"><img src="/a.jpg">'})
    assert images==['https://news.example/a.jpg','https://news.example/b.jpg']
    pid=post(image_url=images[0],image_urls=json.dumps(images))
    assert client.put(f'/api/posts/{pid}',json={'text':COPY,'image_url':''}).status_code==200
    saved=next(p for p in client.get('/api/posts').json() if p['id']==pid)
    assert json.loads(saved['image_urls'])==[]


def test_discover_max_chat_ids(client,monkeypatch):
    requests=[]
    def handler(request):
        requests.append(request)
        if request.url.path=='/updates':
            return httpx.Response(200,json={'updates':[
                {'update_type':'bot_added','chat_id':-123,'is_channel':True},
                {'update_type':'message_created','message':{'recipient':{'chat_id':-456}}},
                {'update_type':'bot_started','chat_id':789},
                {'update_type':'bot_removed','chat_id':-999},
            ],'marker':42})
        if request.url.path=='/chats/-999':return httpx.Response(403)
        cid=int(request.url.path.rsplit('/',1)[1])
        return httpx.Response(200,json={'chat_id':cid,'type':'dialog' if cid==789 else 'channel','status':'active',
                                      'title':'TechPulse','link':'https://max.ru/channel_TechPulse'})
    monkeypatch.setattr(module.httpx,'AsyncClient',lambda *a,**kw:CLIENT(transport=httpx.MockTransport(handler),**kw))
    result=client.get('/api/max/chats')
    assert result.status_code==200
    assert {c['chat_id'] for c in result.json()['chats']}=={'-123','-456'}
    assert all(r.method=='GET' and r.url.path!='/chats' for r in requests)
    assert 'marker' not in requests[0].url.params
    assert requests[0].url.params['timeout']=='0'
    # Cached discovery survives loss of the original events, and is scoped to the token.
    def no_updates(request):
        if request.url.path=='/updates':return httpx.Response(200,json={'updates':[]})
        return handler(request)
    monkeypatch.setattr(module.httpx,'AsyncClient',lambda *a,**kw:CLIENT(transport=httpx.MockTransport(no_updates),**kw))
    assert len(client.get('/api/max/chats').json()['chats'])==2
    monkeypatch.setattr(module,'TOKEN','another-token')
    assert client.get('/api/max/chats').json()['chats']==[]
    monkeypatch.setattr(module,'TOKEN','')
    assert client.get('/api/max/chats').status_code==400


@pytest.mark.parametrize('status,expected',[(401,'токен'),(403,'запретил'),(404,'не означает неверный токен'),(409,'другой сервис'),(429,'Лимит'),(500,'HTTP 500')])
def test_max_chat_errors_explain_status_without_leaking_body(client,monkeypatch,status,expected):
    def handler(request):
        return httpx.Response(status,json={'code':'method.not.found','message':'secret-value-do-not-show'})
    monkeypatch.setattr(module.httpx,'AsyncClient',lambda *a,**kw:CLIENT(transport=httpx.MockTransport(handler),**kw))
    result=client.get('/api/max/chats')
    assert result.status_code==502 and expected in result.json()['detail']
    assert 'secret-value' not in result.text


def test_max_empty_updates_and_authentication(client,monkeypatch):
    requests=[]
    def handler(request):
        requests.append(request)
        return httpx.Response(200,json={'updates':[],'marker':42})
    monkeypatch.setattr(module.httpx,'AsyncClient',lambda *a,**kw:CLIENT(transport=httpx.MockTransport(handler),**kw))
    assert client.get('/api/max/chats',headers={'Authorization':''}).status_code==401
    assert not requests
    result=client.get('/api/max/chats')
    assert result.status_code==200 and result.json()['chats']==[]
    assert 'вручную' in result.json()['hint']


def test_max_chat_discovery_connection_and_invalid_response(client,monkeypatch):
    def offline(request): raise httpx.ConnectError('secret-value',request=request)
    monkeypatch.setattr(module.httpx,'AsyncClient',lambda *a,**kw:CLIENT(transport=httpx.MockTransport(offline),**kw))
    result=client.get('/api/max/chats')
    assert result.status_code==502 and 'соединение' in result.json()['detail']
    assert 'secret-value' not in result.text
    monkeypatch.setattr(module.httpx,'AsyncClient',lambda *a,**kw:CLIENT(transport=httpx.MockTransport(lambda r:httpx.Response(200,json={'unexpected':True})),**kw))
    result=client.get('/api/max/chats')
    assert result.status_code==502 and 'формат' in result.json()['detail']


def test_instant_pipeline_publishes_each_approval_without_reserve_or_cap(client,monkeypatch):
    configure(client,monkeypatch,True)
    calls=network(monkeypatch,rss=pool_feed(25))
    client.post('/api/sources',json={'channel_id':1,'url':'https://news.example/rss'})
    asyncio.run(module.run_collection())
    sends=[i for i,r in enumerate(calls) if r.url.host=='platform-api.max.ru']
    generations=[i for i,r in enumerate(calls) if r.url.path=='/api/generate']
    assert len(sends)==25
    assert sends[0]<generations[3]  # First post is sent before the next story's inference.
    with module.db() as con:
        assert con.execute("SELECT COUNT(*) FROM posts WHERE status='published'").fetchone()[0]==25
        assert con.execute("SELECT COUNT(*) FROM posts WHERE ai_status='standby'").fetchone()[0]==0
    asyncio.run(module.automation_tick())
    assert len([r for r in calls if r.url.host=='platform-api.max.ru'])==25


def test_prepared_batch_drains_newest_first_without_cooldown(client,monkeypatch):
    configure(client,monkeypatch,True)
    now=datetime.now(timezone.utc)
    for i in range(4):
        post(title=f'Компания выпустила модель ИИ версии {i}',fingerprint=str(i),url=f'https://news.example/{i}',source_published_at=(now-timedelta(minutes=i)).isoformat())
    sends=[]
    def handler(request):
        with module.db() as con:
            sends.append(con.execute("SELECT title FROM posts WHERE status='publishing'").fetchone()[0])
        return httpx.Response(200,json={'message':{'body':{'mid':str(len(sends))}}})
    monkeypatch.setattr(module.httpx,'AsyncClient',lambda *a,**kw:CLIENT(transport=httpx.MockTransport(handler),**kw))
    asyncio.run(module.automation_tick())
    assert len(sends)==4
    assert [title[-1] for title in sends]==['0','1','2','3']


def test_new_arrivals_take_priority_during_inference(client,monkeypatch):
    configure(client,monkeypatch,True)
    network(monkeypatch)
    now=datetime.now(timezone.utc)
    first=post(title='ИИ модель версии 1',fingerprint='1',url='https://news.example/1',ai_status='pending')
    post(title='ИИ модель версии 2',fingerprint='2',url='https://news.example/2',ai_status='pending',source_published_at=(now-timedelta(hours=1)).isoformat())
    order=[]
    async def fake_review(title,*args,**kwargs):
        order.append(title)
        if len(order)==1:
            post(title='ИИ модель версии 3',fingerprint='3',url='https://news.example/3',ai_status='pending',source_published_at=(now+timedelta(minutes=1)).isoformat())
        else:
            with module.db() as con: assert con.execute('SELECT status FROM posts WHERE id=?',(first,)).fetchone()[0]=='published'
        return editorial.Verdict(publishable=True,score=90,reason='Подтверждено',text=COPY,event=title,evidence=SOURCE,headline=COPY.split('\n\n')[0])
    monkeypatch.setattr(module,'review',fake_review)
    asyncio.run(module.review_pending())
    assert order==['ИИ модель версии 1','ИИ модель версии 3','ИИ модель версии 2']


def test_feed_poll_continues_while_model_is_busy(client,monkeypatch):
    called=[]
    async def collect(**kwargs):
        called.append(kwargs)
        module.COLLECT_STATE['running']=False
    monkeypatch.setattr(module,'run_collection',collect)
    module.REVIEW_STATE['running']=True
    asyncio.run(module.tick())
    assert called==[{'review_after':False}]


@pytest.mark.parametrize('headline',['','Новая модель для документов'])
def test_model_explicitly_chooses_headline_and_publication_respects_it(client,monkeypatch,headline):
    body='Разработчики открыли доступ к системе обработки документов. Испытания прошли на русском языке.'
    calls=network(monkeypatch,copy=body,headline=headline)
    configure(client,monkeypatch)
    pid=post(ai_status='pending')
    assert client.post(f'/api/posts/{pid}/publish').status_code==200
    saved=next(p for p in client.get('/api/posts').json() if p['id']==pid)
    assert saved['headline']==headline
    sent=json.loads(next(r for r in calls if r.url.host=='platform-api.max.ru').content)['text']
    assert sent.startswith('<b>'+headline+'</b>\n\n'+body if headline else body+'\n\n<a ')
    assert sent.count(body)==1


@pytest.mark.parametrize("body",["OpenAI открыла доступ к новой модели для всех пользователей.","ЦБ снизил ставку до 15%."])
def test_short_copy_without_title_or_padding_is_accepted(client,monkeypatch,body):
    network(monkeypatch,copy=body,headline='')
    result=asyncio.run(editorial.review('OpenAI открыла новую модель',SOURCE,'ИИ'))
    assert result.publishable and result.text==body and result.headline==''


@pytest.mark.parametrize('copy',[
    'Какие шансы у России на ближайших турнирах по хоккею?\n\nПрогноз на ЧМ-2027: минимум финал.\n\nКакие шансы у России на ближайших турнирах по хоккею?\n\nПрогноз на ЧМ-2027: минимум финал.',
    COPY+'\n\n'+COPY,
    'Компания открыла доступ к новой модели для всех пользователей. Компания открыла доступ к новой модели для всех пользователей.',
    'MoneyPulse | Финансы\n'+COPY,
])
def test_repeated_or_channel_prefixed_generation_never_approved(client,monkeypatch,copy):
    calls=network(monkeypatch,copy=copy,headline='')
    with pytest.raises(editorial.EditorialUnavailable):asyncio.run(editorial.review('OpenAI выпустила модель',SOURCE,'ИИ'))
    assert len([r for r in calls if r.url.path=='/api/generate'])==3
    assert not any(r.url.host=='platform-api.max.ru' for r in calls)


def test_duplicate_headline_body_gets_a_corrected_attempt(client,monkeypatch):
    bodies=iter([
        {'headline':'OpenAI открыла доступ к новой модели для всех пользователей','text':'OpenAI открыла доступ к новой модели для всех пользователей. Модель обрабатывает документы и изображения.'},
        {'headline':'','text':'OpenAI открыла доступ к новой модели для всех пользователей. Она обрабатывает документы и изображения.'},
    ])
    original=editorial.generate
    async def generate(client,system,data,schema):
        if schema is editorial.PostCopy:return editorial.PostCopy(**next(bodies))
        return await original(client,system,data,schema)
    network(monkeypatch)
    monkeypatch.setattr(editorial,'generate',generate)
    verdict=asyncio.run(editorial.review('OpenAI выпустила модель',SOURCE,'ИИ'))
    assert verdict.publishable and verdict.headline=='' and verdict.text.count('OpenAI')==1


def test_plain_multi_paragraph_post_is_not_automatically_bold():
    from publication import message_payloads
    text='Компания выпустила новую модель.\n\nДоступ открыли всем пользователям.'
    assert not message_payloads(text,headline='')[0]['text'].startswith('<b>')


def test_model_detected_semantic_repetition_is_not_approved(client,monkeypatch):
    network(monkeypatch,copy=COPY,copy_valid=False)
    with pytest.raises(editorial.EditorialUnavailable):asyncio.run(editorial.review('OpenAI выпустила модель',SOURCE,'ИИ'))


def test_old_duplicate_draft_is_rechecked_before_send(client,monkeypatch):
    configure(client,monkeypatch)
    pid=post(text=COPY+'\n\n'+COPY,editorial_revision=module.NEWS_REVISION-1)
    calls=network(monkeypatch)
    assert client.post(f'/api/posts/{pid}/publish').status_code==200
    sent=json.loads(next(r for r in calls if r.url.host=='platform-api.max.ru').content)['text']
    assert sent.count('Компания выпустила новую модель ИИ')==1
