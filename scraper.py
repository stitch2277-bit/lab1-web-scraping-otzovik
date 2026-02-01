import os
import time
import logging
import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def create_directory_structure():
    base_dir = 'dataset'
    
    try:
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
            logger.info(f"✓ Создана папка: {base_dir}")
        
        for rating in range(1, 6):
            rating_dir = os.path.join(base_dir, str(rating))
            if not os.path.exists(rating_dir):
                os.makedirs(rating_dir)
                logger.info(f"✓ Создана папка: {rating_dir}")
        
        logger.info("✓ Структура папок создана успешно")
        return True
    
    except Exception as e:
        logger.error(f"✗ Ошибка при создании структуры папок: {e}")
        return False

def fetch_page_with_requests(url, attempt=1, max_attempts=3):
    logger.info(f"Получение страницы: {url} (попытка {attempt}/{max_attempts})")
    
    if attempt > max_attempts:
        logger.error("✗ Превышено максимальное количество попыток")
        return None
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3'
    }
    
    try:
        logger.info(" Ожидание 10 секунд перед подключением...")
        time.sleep(10)
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        logger.info("✓ Страница успешно загружена")
        return response.text
    
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            logger.error("✗ Ошибка 403: Доступ запрещён")
        elif e.response.status_code == 429:
            logger.error("✗ Ошибка 429: Слишком много запросов")
        else:
            logger.error(f"✗ HTTP ошибка {e.response.status_code}: {e}")
        return None
    
    except requests.exceptions.Timeout:
        logger.warning("⚠ Таймаут запроса")
        if attempt < max_attempts:
            time.sleep(5)
            return fetch_page_with_requests(url, attempt + 1, max_attempts)
        return None
    
    except requests.exceptions.RequestException as e:
        logger.error(f"✗ Ошибка запроса: {e}")
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
        
        logger.info(f"Найдено отзывов на странице: {len(review_blocks)}")
        
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
                    'author': author
                }
                
                reviews.append(review_data)
                logger.info(f"✓ Отзыв: {title[:50]}... (Рейтинг: {rating}, Автор: {author})")
                
            except Exception as e:
                logger.warning(f"⚠ Ошибка при парсинге отзыва: {e}")
                continue
        
        logger.info(f"Успешно извлечено {len(reviews)} отзывов")
        return reviews
    
    except Exception as e:
        logger.error(f"✗ Ошибка при парсинге: {e}")
        return []

def save_review_to_file(review, rating, review_number):
    if not rating or rating < 1 or rating > 5:
        logger.warning(f"⚠ Пропущен отзыв с некорректным рейтингом: {rating}")
        return False
    
    filename = f"{review_number:04d}.txt"
    filepath = os.path.join('dataset', str(rating), filename)
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"Заголовок: {review['title']}\n")
            f.write(f"Автор: {review['author']}\n")
            f.write(f"Дата: {review['date']}\n")
            f.write(f"Рейтинг: {review['rating']} звезд(ы)\n")
            f.write("\n" + "="*50 + "\n\n")
            f.write(f"Текст отзыва:\n{review['teaser']}\n")
        
        logger.info(f"✓ Сохранен: {filepath}")
        return True
    
    except PermissionError:
        logger.error(f"✗ Ошибка доступа к файлу: {filepath}")
        return False
    
    except Exception as e:
        logger.error(f"✗ Ошибка при сохранении файла {filepath}: {e}")
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
    
    logger.info(f"\n Целевой сайт: {base_url}")
    
    html = fetch_page_with_requests(base_url)
    
    if not html:
        logger.error("✗ Не удалось получить данные с сайта")
        return
    
    reviews = parse_reviews(html)
    
    if not reviews:
        logger.error("✗ Отзывы не найдены")
        return
    
    logger.info(f"\n Всего найдено отзывов: {len(reviews)}")
    
    saved_count = 0
    for i, review in enumerate(reviews, start=1):
        if save_review_to_file(review, review['rating'], i):
            saved_count += 1
    
    logger.info("="*60)
    logger.info(f" Завершено! Сохранено {saved_count} отзывов")
    logger.info(f" Файлы сохранены в папку: dataset/")
    logger.info(f" Лог сохранен в файл: scraper.log")
    logger.info("="*60)

if __name__ == "__main__":
    main()