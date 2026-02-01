import os
import time
import logging
import random
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def create_directory_structure(base_dir='dataset'):
    try:
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
            logger.info("✓ Создана папка: %s", base_dir)
        
        for rating in range(1, 6):
            rating_dir = os.path.join(base_dir, str(rating))
            if not os.path.exists(rating_dir):
                os.makedirs(rating_dir)
                logger.info("✓ Создана папка: %s", rating_dir)
        
        logger.info("✓ Структура папок создана успешно")
        return True
    
    except Exception as e:
        logger.error("✗ Ошибка при создании структуры папок: %s", e)
        return False

def fetch_page_with_requests(url, attempt=1, max_attempts=3):
    logger.info("Получение страницы: %s (попытка %d/%d)", url, attempt, max_attempts)
    
    if attempt > max_attempts:
        logger.error("✗ Превышено максимальное количество попыток")
        return None
    
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
    ]
    
    headers = {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    
    try:
        delay = random.uniform(10.0, 12.0)
        logger.info(" Ожидание %.1f секунд...", delay)
        time.sleep(delay)
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        if 'captcha' in response.text.lower() or 'доступ запрещен' in response.text.lower():
            logger.warning("⚠ Обнаружена защита от парсинга")
            if attempt < max_attempts:
                time.sleep(15)
                return fetch_page_with_requests(url, attempt + 1, max_attempts)
            return None
        
        logger.info("✓ Страница успешно загружена")
        return response.text
    
    except requests.exceptions.HTTPError as e:
        logger.error("✗ HTTP ошибка %s: %s", e.response.status_code, e)
        return None
    
    except requests.exceptions.Timeout:
        logger.warning("⚠ Таймаут запроса")
        if attempt < max_attempts:
            time.sleep(5)
            return fetch_page_with_requests(url, attempt + 1, max_attempts)
        return None
    
    except requests.exceptions.RequestException as e:
        logger.error("✗ Ошибка запроса: %s", e)
        return None

def fetch_full_review_text(review_url):
    try:
        html = fetch_page_with_requests(review_url)
        
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        
        review_body = soup.find('div', class_='review-body')
        
        if not review_body:
            review_body = soup.find('div', class_='review-full')
        
        if not review_body:
            review_body = soup.find('div', itemprop='reviewBody')
        
        if review_body:
            for elem in review_body.find_all(['script', 'style', 'button', 'a']):
                elem.decompose()
            
            full_text = review_body.get_text(separator='\n', strip=True)
            return full_text
        
        return None
    
    except Exception as e:
        logger.warning("⚠ Ошибка при получении полного текста: %s", e)
        return None

def parse_reviews(html):
    if not html:
        logger.error("✗ Нет данных для парсинга")
        return []
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        reviews = []
        
        all_items = soup.find_all('div', class_='item')
        review_blocks = [item for item in all_items if 'status4' in item.get('class', []) or 'status10' in item.get('class', [])]
        
        logger.info("Найдено отзывов на странице: %d", len(review_blocks))
        
        for block in review_blocks[:10]:
            try:
                rating_meta = block.find('meta', itemprop='reviewRating')
                if rating_meta and rating_meta.get('content'):
                    rating = int(float(rating_meta['content']))
                else:
                    rating_elem = block.find('div', class_='rating-score')
                    if rating_elem:
                        rating_text = rating_elem.get_text(strip=True)
                        rating = int(''.join(filter(str.isdigit, rating_text))) if rating_text else None
                    else:
                        rating = None
                
                title_elem = block.find('a', class_='review-title')
                title = title_elem.get_text(strip=True) if title_elem else "Без названия"
                
                link_elem = block.find('a', class_='review-title')
                link = link_elem['href'] if link_elem and 'href' in link_elem.attrs else ""
                
                if link and not link.startswith('http'):
                    link = f"https://otzovik.com{link}"
                
                teaser_elem = block.find('div', class_='review-teaser')
                teaser = teaser_elem.get_text(strip=True) if teaser_elem else "Текст отзыва отсутствует"
                
                date_elem = block.find('div', class_='review-postdate')
                date = date_elem.get_text(strip=True) if date_elem else "Дата не указана"
                
                author_elem = block.find('span', itemprop='name')
                author = author_elem.get_text(strip=True) if author_elem else "Аноним"
                
                review_data = {
                    'rating': rating,
                    'title': title,
                    'teaser': teaser,
                    'date': date,
                    'author': author,
                    'link': link,
                    'full_text': None
                }
                
                reviews.append(review_data)
                logger.info("✓ Отзыв: %s... (Рейтинг: %s, Автор: %s)", title[:30], rating, author)
                
            except Exception as e:
                logger.warning("⚠ Ошибка при парсинге отзыва: %s", e)
                continue
        
        logger.info("Успешно извлечено %d отзывов", len(reviews))
        return reviews
    
    except Exception as e:
        logger.error("✗ Ошибка при парсинге: %s", e)
        return []

def save_review_to_file(review, rating, review_number, base_dir='dataset'):
    if not isinstance(rating, int) or rating < 1 or rating > 5:
        logger.warning("⚠ Пропущен отзыв #%d с некорректным рейтингом: %s", review_number, rating)
        return False
    
    filename = f"{review_number:04d}.txt"
    filepath = os.path.join(base_dir, str(rating), filename)
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("="*60 + "\n")
            f.write(f"ЗАГОЛОВОК: {review['title']}\n")
            f.write("="*60 + "\n\n")
            f.write(f"Автор: {review['author']}\n")
            f.write(f"Дата: {review['date']}\n")
            f.write(f"Рейтинг: {review['rating']} звезд(ы)\n")
            f.write(f"Ссылка: {review['link']}\n\n")
            f.write("-"*60 + "\n")
            f.write("ТЕКСТ ОТЗЫВА:\n")
            f.write("-"*60 + "\n\n")
            
            text_to_save = review.get('full_text', review['teaser'])
            f.write(text_to_save + "\n\n")
            
            f.write("="*60 + "\n")
            f.write(f"Сохранено: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("="*60 + "\n")
        
        logger.info("✓ Сохранен: %s", filepath)
        return True
    
    except PermissionError:
        logger.error("✗ Ошибка доступа к файлу: %s", filepath)
        return False
    
    except Exception as e:
        logger.error("✗ Ошибка при сохранении файла %s: %s", filepath, e)
        return False

def main():
    logger.info("="*60)
    logger.info("Web-Scraper для otzovik.com")
    logger.info("Парсинг отзывов о Сбербанке")
    logger.info("="*60)
    
    if not create_directory_structure():
        logger.error("✗ Не удалось создать структуру папок")
        return
    
    base_url = "https://otzovik.com/reviews/sberbank_rossii/"
    
    logger.info("\n Целевой сайт: %s", base_url)
    
    html = fetch_page_with_requests(base_url)
    
    if not html:
        logger.error("✗ Не удалось получить данные с сайта")
        return
    
    reviews = parse_reviews(html)
    
    if not reviews:
        logger.error("✗ Отзывы не найдены")
        return
    
    logger.info("\n Всего найдено отзывов: %d", len(reviews))
    
    logger.info("\n Получение полных текстов отзывов...")
    
    for i, review in enumerate(tqdm(reviews, desc="Получение полных текстов"), start=1):
        if review['link']:
            full_text = fetch_full_review_text(review['link'])
            if full_text:
                review['full_text'] = full_text
                logger.debug("✓ Получен полный текст для отзыва %d", i)
        
        if i < len(reviews):
            time.sleep(random.uniform(6.0, 8.0))
    
    saved_count = 0
    logger.info("\n💾 Сохранение отзывов в файлы...")
    
    for i, review in enumerate(tqdm(reviews, desc="Сохранение отзывов"), start=1):
        if save_review_to_file(review, review['rating'], i):
            saved_count += 1
    
    logger.info("="*60)
    logger.info(" ЗАВЕРШЕНО!")
    logger.info(" Файлы сохранены в папку: dataset/")
    logger.info(" Сохранено %d отзывов", saved_count)
    logger.info(" Лог сохранен в файл: scraper.log")
    logger.info("="*60)

if __name__ == "__main__":
    main()