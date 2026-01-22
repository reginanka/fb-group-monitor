import feedparser
import os
import re
from datetime import datetime, timedelta
from supabase import create_client, Client
import requests

# Supabase
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')  # anon/service_role key
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

RSS_URL = os.getenv('RSS_URL')
TG_BOT_TOKEN = os.getenv('TG_BOT_TOKEN')
TG_CHAT_ID = os.getenv('TG_CHAT_ID')

def extract_first_sentence(text, limit=100):
    """Перше речення або перші 100 символів"""
    if not text:
        return ""
    # Видаляємо HTML/лінки
    text = re.sub(r'<[^>]+>|https?://\S+', '', text)
    # Перше речення
    match = re.match(r'^[^.!?]+[.!?]', text)
    if match:
        return match.group(0)[:limit]
    return text[:limit].strip()

def cleanup_old_posts():
    """Видаляє пости старші 30 днів"""
    cutoff = (datetime.now() - timedelta(days=30)).isoformat()
    result = supabase.table('group_posts').delete().lt('created_at', cutoff).execute()
    print(f"Видалено {len(result.data)} старих постів")

def check_spam_patterns(user_id, first_sentence):
    """Перевіряє: багато постів + схожий текст"""
    # Останні пости користувача за 24 год
    cutoff = (datetime.now() - timedelta(hours=24)).isoformat()
    result = supabase.table('group_posts')\
        .select('first_sentence, created_at')\
        .eq('user_id', user_id)\
        .gte('created_at', cutoff)\
        .execute()
    
    posts = result.data
    if len(posts) > 4:  # >4 пости/день
        # Чи схожі тексти?
        similar = sum(1 for p in posts if p['first_sentence'] == first_sentence)
        if similar > 2:  # Повторює той самий текст
            return True, len(posts), "Дублікат тексту"
        return True, len(posts), "Багато постів"
    return False, 0, ""

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    requests.post(url, data={'chat_id': TG_CHAT_ID, 'text': message, 'parse_mode': 'HTML'})

def parse_rss():
    feed = feedparser.parse(RSS_URL)
    
    for entry in feed.entries[-30:]:  # Останні 30
        link = entry.link
        author = entry.get('author', 'Unknown')
        
        # Витягуємо user_id з лінку (якщо є)
        user_id_match = re.search(r'user/(\d+)|/profile\.php\?id=(\d+)', link)
        user_id = user_id_match.group(1) or user_id_match.group(2) if user_id_match else author
        
        # Перевіряємо чи є в БД
        existing = supabase.table('group_posts').select('id').eq('post_link', link).execute()
        if existing.data:
            continue  # Вже обробили
        
        # Перше речення
        text = entry.get('summary', entry.get('title', ''))
        first_sent = extract_first_sentence(text)
        
        # Зберігаємо
        supabase.table('group_posts').insert({
            'user_id': user_id,
            'user_name': author,
            'post_link': link,
            'first_sentence': first_sent,
        }).execute()
        
        # Чекаємо спам
        is_spam, count, reason = check_spam_patterns(user_id, first_sent)
        if is_spam:
            message = f"🚨 <b>Підозріла активність</b>\n\n"\
                      f"👤 {author} (ID: {user_id})\n"\
                      f"📊 Постів за 24 год: {count}\n"\
                      f"⚠️ Причина: {reason}\n"\
                      f"📝 Текст: <i>{first_sent}</i>\n"\
                      f"🔗 <a href='{link}'>Переглянути пост</a>"
            send_telegram(message)
    
    print(f"RSS перевірено: {len(feed.entries)} постів")

def main():
    cleanup_old_posts()  # Спочатку чистимо
    parse_rss()

if __name__ == "__main__":
    main()
