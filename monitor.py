import requests
from bs4 import BeautifulSoup
import os
import re
from datetime import datetime, timedelta
from supabase import create_client, Client
import time

# Supabase
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Telegram
TG_BOT_TOKEN = os.getenv('TG_BOT_TOKEN')
TG_CHAT_ID = os.getenv('TG_CHAT_ID')

# Facebook група
GROUP_ID = os.getenv('GROUP_ID')
GROUP_URL = f"https://mbasic.facebook.com/groups/{GROUP_ID}"

def get_last_checkpoint():
    """Отримує timestamp останньої перевірки"""
    result = supabase.table('monitor_state')\
        .select('last_check_time')\
        .eq('group_id', GROUP_ID)\
        .execute()
    
    if result.data:
        return datetime.fromisoformat(result.data[0]['last_check_time'])
    else:
        # Перший запуск — беремо 3 дні назад для тесту
        return datetime.now() - timedelta(days=3)

def update_checkpoint():
    """Оновлює timestamp останньої перевірки"""
    now = datetime.now().isoformat()
    supabase.table('monitor_state').upsert({
        'group_id': GROUP_ID,
        'last_check_time': now
    }).execute()

def scrape_facebook_posts(since_time):
    """
    Парсить пости з mbasic.facebook.com
    since_time: datetime — з якого часу читати
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    }
    
    posts = []
    url = GROUP_URL
    max_pages = 3
    
    for page in range(max_pages):
        try:
            print(f"🔍 Завантажую сторінку {page + 1}: {url[:80]}...")
            response = requests.get(url, headers=headers, timeout=15)
            
            print(f"📡 Статус: {response.status_code}, Розмір: {len(response.text)} байт")
            
            if response.status_code != 200:
                print(f"❌ FB повернув {response.status_code}, зупиняємось")
                break
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Дебаг: шукаємо різні типи контейнерів
            post_divs = soup.find_all('div', {'data-ft': True})
            article_tags = soup.find_all('article')
            
            print(f"📦 Знайдено: {len(post_divs)} divs[data-ft], {len(article_tags)} articles")
            
            if not post_divs and not article_tags:
                print("⚠️ Не знайдено постів. Можливо потрібна авторизація або селектори змінились")
                # Зберігаємо HTML для аналізу
                with open('fb_debug.html', 'w', encoding='utf-8') as f:
                    f.write(response.text[:5000])
                print("💾 Перші 5000 символів збережено у fb_debug.html")
                break
            
            found_old_post = False
            
            for post_div in post_divs:
                # Витягуємо час поста
                time_elem = post_div.find('abbr')
                if not time_elem:
                    continue
                
                post_time_str = time_elem.get_text()
                post_time = parse_fb_time(post_time_str)
                
                print(f"⏰ Пост: {post_time_str} -> {post_time}")
                
                if post_time < since_time:
                    found_old_post = True
                    print(f"⏹️ Знайшли старий пост, зупиняємось")
                    break
                
                # Витягуємо автора
                author_elem = post_div.find('h3')
                author = author_elem.get_text().strip() if author_elem else 'Unknown'
                
                # Витягуємо текст
                content_elem = post_div.find('div', {'data-ft': True})
                text = content_elem.get_text().strip() if content_elem else ''
                
                # Витягуємо посилання на пост
                link_elem = post_div.find('a', href=True)
                post_link = GROUP_URL
                if link_elem and 'href' in link_elem.attrs:
                    href = link_elem['href']
                    if href.startswith('/'):
                        post_link = 'https://mbasic.facebook.com' + href
                
                # Витягуємо user_id
                user_id = extract_user_id(post_div) or author
                
                posts.append({
                    'user_id': user_id,
                    'user_name': author,
                    'text': text[:500],  # Обмежуємо довжину
                    'link': post_link,
                    'timestamp': post_time
                })
                
                print(f"✅ Додано пост від {author}")
            
            if found_old_post:
                break
            
            # Наступна сторінка
            next_link = soup.find('a', string=re.compile('See more posts|Показати більше|Show more'))
            if not next_link or not next_link.get('href'):
                print("🏁 Немає наступної сторінки")
                break
            
            url = 'https://mbasic.facebook.com' + next_link['href']
            time.sleep(3)  # Збільшена затримка
            
        except Exception as e:
            print(f"❌ Помилка парсингу: {e}")
            import traceback
            traceback.print_exc()
            break
    
    return posts

def parse_fb_time(time_str):
    """
    Парсить час з Facebook
    """
    now = datetime.now()
    time_str_lower = time_str.lower()
    
    try:
        if 'min' in time_str_lower or 'хв' in time_str_lower:
            match = re.search(r'(\d+)', time_str)
            if match:
                mins = int(match.group(1))
                return now - timedelta(minutes=mins)
        elif 'hr' in time_str_lower or 'hour' in time_str_lower or 'год' in time_str_lower:
            match = re.search(r'(\d+)', time_str)
            if match:
                hrs = int(match.group(1))
                return now - timedelta(hours=hrs)
        elif 'yesterday' in time_str_lower or 'вчора' in time_str_lower:
            return now - timedelta(days=1)
        elif 'day' in time_str_lower or 'дн' in time_str_lower:
            match = re.search(r'(\d+)', time_str)
            if match:
                days = int(match.group(1))
                return now - timedelta(days=days)
    except:
        pass
    
    # За замовчуванням - 1 година назад
    return now - timedelta(hours=1)

def extract_user_id(post_div):
    """Витягує user_id з посилання на профіль"""
    profile_link = post_div.find('a', href=re.compile(r'/profile\.php\?id=|/[^/]+\?'))
    if profile_link:
        match = re.search(r'id=(\d+)', profile_link['href'])
        if match:
            return match.group(1)
    return None

def extract_first_sentence(text, limit=100):
    """Перше речення або перші 100 символів"""
    if not text:
        return ""
    text = re.sub(r'<[^>]+>|https?://\S+', '', text)
    match = re.match(r'^[^.!?]+[.!?]', text)
    if match:
        return match.group(0)[:limit]
    return text[:limit].strip()

def check_spam_patterns(user_id, first_sentence):
    """Детекція спаму"""
    cutoff = (datetime.now() - timedelta(hours=24)).isoformat()
    result = supabase.table('group_posts')\
        .select('first_sentence, created_at')\
        .eq('user_id', user_id)\
        .gte('created_at', cutoff)\
        .execute()
    
    posts = result.data
    
    if len(posts) > 4:
        similar = sum(1 for p in posts if p['first_sentence'] == first_sentence)
        if similar > 2:
            return True, len(posts), "Дублікат тексту"
        return True, len(posts), "Багато постів"
    
    return False, 0, ""

def send_telegram(message):
    """Надсилає повідомлення в Telegram"""
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, data={
            'chat_id': TG_CHAT_ID,
            'text': message,
            'parse_mode': 'HTML',
            'disable_web_page_preview': True
        }, timeout=5)
    except Exception as e:
        print(f"❌ Помилка Telegram: {e}")

def process_posts(posts):
    """Обробка та збереження постів"""
    new_count = 0
    spam_count = 0
    
    for post in posts:
        # Перевіряємо чи є в БД
        existing = supabase.table('group_posts')\
            .select('id')\
            .eq('post_link', post['link'])\
            .execute()
        
        if existing.data:
            continue
        
        # Зберігаємо
        first_sent = extract_first_sentence(post['text'])
        try:
            supabase.table('group_posts').insert({
                'user_id': post['user_id'],
                'user_name': post['user_name'],
                'post_link': post['link'],
                'first_sentence': first_sent,
                'created_at': post['timestamp'].isoformat()
            }).execute()
            new_count += 1
        except Exception as e:
            print(f"❌ Помилка запису в БД: {e}")
            continue
        
        # Детекція спаму
        is_spam, count, reason = check_spam_patterns(post['user_id'], first_sent)
        
        if is_spam:
            spam_count += 1
            message = f"🚨 Підозріла активність\n\n"\
                     f"👤 {post['user_name']} (ID: {post['user_id']})\n"\
                     f"📊 Постів за 24 год: {count}\n"\
                     f"⚠️ Причина: {reason}\n"\
                     f"📝 Текст: {first_sent}\n"\
                     f"🔗 <a href='{post['link']}'>Переглянути пост</a>"
            send_telegram(message)
    
    return new_count, spam_count

def main():
    print(f"🚀 Запуск моніторингу групи {GROUP_ID}")
    
    # Отримуємо останній checkpoint
    last_check = get_last_checkpoint()
    print(f"📅 Читаємо пости після {last_check}")
    
    # Парсимо FB
    posts = scrape_facebook_posts(last_check)
    print(f"📄 Знайдено {len(posts)} нових постів")
    
    # Обробляємо
    new_count, spam_count = process_posts(posts)
    
    # Оновлюємо checkpoint
    update_checkpoint()
    
    print(f"✅ Оброблено: {new_count} нових, {spam_count} спамерів")
    
    # Підсумкове повідомлення
    if new_count > 0:
        summary = f"📊 Моніторинг завершено\n"\
                 f"Нових постів: {new_count}\n"\
                 f"Спамерів: {spam_count}"
        send_telegram(summary)

if __name__ == "__main__":
    main()
