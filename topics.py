"""Channel topics and conservative candidate filters; the model makes the final decision."""
import re
PROFILES = {
 'ai': ('NeuroPulse | Нейросети и ИИ', 'искусственный интеллект, нейросети, модели и продукты ИИ', ('https://habr.com/ru/rss/news/?fl=ru','https://www.ixbt.com/export/news.rss','https://3dnews.ru/news/rss')),
 'tech': ('TechPulse | Технологии', 'технологии, гаджеты, программное обеспечение, электроника и научные разработки', ('https://www.ixbt.com/export/news.rss','https://3dnews.ru/news/rss')),
 'finance': ('MoneyPulse | Финансы', 'финансы, банки, ставки, валюты, фондовые рынки и экономика', ('https://www.kommersant.ru/RSS/section-economics.xml',)),
 'sport': ('SportPulse | Спорт', 'спорт, результаты соревнований, турниры, команды и спортсмены', ('https://www.sports.ru/rss/main.xml',)),
 'business': ('BizPulse | Бизнес', 'бизнес, компании, сделки, предпринимательство и корпоративные результаты', ('https://www.kommersant.ru/RSS/section-business.xml',)),
}
PATTERNS = {
 'tech': r'технолог|смартфон|процессор|ноутбук|приложени|обновлен|обновлён|гаджет|электрон|робот|спутник|разработ|софт|android|apple|samsung|microsoft',
 'finance': r'финанс|банк|ставк|валют|рубл|доллар|инфляц|акци[яий]|бирж|инвест|эконом|бюджет|облигац',
 'sport': r'спорт|матч|турнир|чемпион|футбол|хокке|теннис|баскетбол|олимпи|сборн|гонк|медал|кубок',
 'business': r'бизнес|компани|сделк|выручк|прибыл|предприним|стартап|корпорац|производств|ритейл|продаж|слияни|поглощени',
}
def brief_for(topic):
 return 'Актуальные новости по теме: '+PROFILES[topic][1]+'. Конкретные новые события и проверяемые факты. Без инструкций, рекламы и пересказов статей.'
def matches_topic(title, summary, topic='ai'):
 if topic == 'ai':
  from editorial import is_ai_story
  return is_ai_story(title, summary)
 return bool(re.search(PATTERNS[topic], title+' '+summary[:12000], re.I))

CHANNEL_URLS = {topic:'https://max.ru/channel_'+name.split(' | ')[0] for topic,(name,_,_) in PROFILES.items()}
