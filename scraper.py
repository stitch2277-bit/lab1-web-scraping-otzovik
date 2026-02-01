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

class OtzovikScraper:
    """Парсер отзывов с сайта otzovik.com"""
    
    def __init__(self, base_url, output_dir='dataset', pages_per_rating=13):
        self.base_url = base_url
        self.output_dir = output_dir
        self.pages_per_rating = pages_per_rating
        self.stats = {
            'total_reviews': 0,
            'saved_reviews': 0,
            'errors': 0
        }
    
    def create_directory_structure(self):
        """Создание структуры папок для сохранения отзывов"""
        try:
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)
                logger.info("✓ Создана папка: %s", self.output_dir)
            
            for rating in range(1, 6):
                rating_dir = os.path.join(self.output_dir, str(rating))
                if not os.path.exists(rating_dir):
                    os.makedirs(rating_dir)
                    logger.info("✓ Создана папка: %s", rating_dir)
            
            logger.info("✓ Структура папок создана успешно")
            return True
        
        except Exception as e:
            logger.error("✗ Ошибка при создании структуры папок: %s", e)
            return False
    
    def fetch_page(self, url, attempt=1, max_attempts=3):
        """Получение HTML страницы с повторными попытками"""
        logger.info("Получение страницы: %s (попытка %d/%d)", url, attempt, max_attempts)
        
        if attempt > max_attempts:
            logger.error("✗ Превышено максимальное количество попыток")
            return None
        
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0'
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
            logger.info("⏳ Ожидание %.1f секунд...", delay)
            time.sleep(delay)
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            if 'captcha' in response.text.lower() or 'доступ запрещен' in response.text.lower():
                logger.warning("⚠ Обнаружена защита от парсинга")
                if attempt < max_attempts:
                    time.sleep(15)
                    return self.fetch_page(url, attempt + 1, max_attempts)
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
                return self.fetch_page(url, attempt + 1, max_attempts)
            return None
        
        except requests.exceptions.RequestException as e:
            logger.error("✗ Ошибка запроса: %s", e)
            return None
    
    def parse_reviews_from_page(self, html):
        """Парсинг отзывов из HTML страницы"""
        if not html:
            logger.error("✗ Нет данных для парсинга")
            return []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            reviews = []
            
            # Находим все блоки отзывов по классам
            review_blocks = soup.find_all('div', class_=['item status4', 'item status10'])
            
            # Альтернативный поиск
            if not review_blocks:
                all_items = soup.find_all('div', class_='item')
                review_blocks = [item for item in all_items if 'status4' in item.get('class', []) or 'status10' in item.get('class', [])]
            
            logger.info("Найдено отзывов на странице: %d", len(review_blocks))
            
            for block in review_blocks:
                try:
                    # Получаем рейтинг
                    rating_elem = block.find('div', class_='rating-score')
                    if rating_elem:
                        rating_text = rating_elem.get_text(strip=True)
                        rating = int(''.join(filter(str.isdigit, rating_text))) if rating_text else None
                    else:
                        rating = None
                    
                    # Получаем заголовок
                    title_elem = block.find('a', class_='review-title')
                    title = title_elem.get_text(strip=True) if title_elem else "Без названия"
                    
                    # Получаем краткий текст
                    teaser_elem = block.find('div', class_='review-teaser')
                    teaser = teaser_elem.get_text(strip=True) if teaser_elem else "Текст отзыва отсутствует"
                    
                    # Получаем дату
                    date_elem = block.find('div', class_='review-postdate')
                    date = date_elem.get_text(strip=True) if date_elem else "Дата не указана"
                    
                    # Получаем автора
                    author_elem = block.find('span', itemprop='name')
                    author = author_elem.get_text(strip=True) if author_elem else "Аноним"
                    
                    # Получаем ссылку на полный отзыв
                    link_elem = block.find('a', class_='review-title')
                    link = link_elem['href'] if link_elem and 'href' in link_elem.attrs else ""
                    
                    if link and not link.startswith('http'):
                        link = f"https://otzovik.com{link}"
                    
                    # Формируем данные отзыва
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
                    logger.debug("✓ Отзыв: %s... (Рейтинг: %s)", title[:30], rating)
                    
                except Exception as e:
                    logger.warning("⚠ Ошибка при парсинге отзыва: %s", e)
                    self.stats['errors'] += 1
                    continue
            
            logger.info("Успешно извлечено %d отзывов", len(reviews))
            return reviews
        
        except Exception as e:
            logger.error("✗ Ошибка при парсинге: %s", e)
            return []
    
    def save_review_to_file(self, review, rating, review_number):
        """Сохранение отзыва в файл с ведущими нулями"""
        if not isinstance(rating, int) or rating < 1 or rating > 5:
            logger.warning("⚠ Пропущен отзыв #%d с некорректным рейтингом: %s", review_number, rating)
            return False
        
        filename = f"{review_number:04d}.txt"
        filepath = os.path.join(self.output_dir, str(rating), filename)
        
        try:
            # Обеспечиваем, что текст отзыва не будет None
            text_to_save = review.get('full_text') or review.get('teaser') or "Текст отзыва отсутствует"
            title = review.get('title', 'Без названия')
            author = review.get('author', 'Аноним')
            date = review.get('date', 'Дата не указана')
            link = review.get('link', '')
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write("="*60 + "\n")
                f.write(f"ЗАГОЛОВОК: {title}\n")
                f.write("="*60 + "\n\n")
                f.write(f"Автор: {author}\n")
                f.write(f"Дата: {date}\n")
                f.write(f"Рейтинг: {rating} звезд(ы)\n")
                if link:
                    f.write(f"Ссылка: {link}\n")
                f.write("\n" + "-"*60 + "\n")
                f.write("ТЕКСТ ОТЗЫВА:\n")
                f.write("-"*60 + "\n\n")
                f.write(text_to_save + "\n\n")
                f.write("="*60 + "\n")
                f.write(f"Сохранено: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("="*60 + "\n")
            
            self.stats['saved_reviews'] += 1
            logger.info("✓ Сохранен: %s", filepath)
            return True
        
        except PermissionError:
            logger.error("✗ Ошибка доступа к файлу: %s", filepath)
            self.stats['errors'] += 1
            return False
        
        except Exception as e:
            logger.error("✗ Ошибка при сохранении файла %s: %s", filepath, e)
            self.stats['errors'] += 1
            return False
    
    def scrape_reviews_by_rating(self, rating_value):
        """Парсинг отзывов определённого рейтинга"""
        logger.info("\n" + "="*60)
        logger.info("Парсинг отзывов с рейтингом: %d звезд", rating_value)
        logger.info("="*60 + "\n")
        
        all_reviews = []
        
        for page_num in tqdm(range(1, self.pages_per_rating + 1), desc=f"Рейтинг {rating_value}", unit="стр"):
            if page_num == 1:
                url = f"{self.base_url}?ratio={rating_value}"
            else:
                url = f"{self.base_url}{page_num}/?ratio={rating_value}"
            
            logger.info("📄 Страница %d/%d: %s", page_num, self.pages_per_rating, url)
            
            html = self.fetch_page(url)
            
            if html:
                reviews = self.parse_reviews_from_page(html)
                all_reviews.extend(reviews)
                self.stats['total_reviews'] += len(reviews)
            
            if page_num < self.pages_per_rating:
                time.sleep(random.uniform(8.0, 10.0))
        
        return all_reviews
    
    def run(self):
        """Запуск парсинга"""
        logger.info("="*60)
        logger.info("Web-Scraper для otzovik.com")
        logger.info("Парсинг отзывов о Сбербанке")
        logger.info("="*60)
        
        if not self.create_directory_structure():
            logger.error("✗ Не удалось создать структуру папок")
            return
        
        all_reviews_by_rating = {}
        
        for rating in range(1, 6):
            reviews = self.scrape_reviews_by_rating(rating)
            all_reviews_by_rating[rating] = reviews
            logger.info("📊 Рейтинг %d: собрано %d отзывов", rating, len(reviews))
        
        total_collected = sum(len(reviews) for reviews in all_reviews_by_rating.values())
        logger.info("\n📊 Всего собрано отзывов: %d", total_collected)
        
        logger.info("\n💾 Сохранение отзывов в файлы...")
        
        for rating, reviews in all_reviews_by_rating.items():
            logger.info("\nРейтинг %d звезд - сохранение...", rating)
            
            for i, review in enumerate(tqdm(reviews, desc=f"Сохранение {rating}"), start=1):
                self.save_review_to_file(review, rating, i)
        
        logger.info("\n" + "="*60)
        logger.info("✅ ЗАВЕРШЕНО!")
        logger.info("="*60)
        logger.info("📁 Файлы сохранены в папку: %s/", self.output_dir)
        logger.info("📄 Всего собрано: %d отзывов", self.stats['total_reviews'])
        logger.info("📄 Успешно сохранено: %d отзывов", self.stats['saved_reviews'])
        logger.info("📄 Ошибок: %d", self.stats['errors'])
        if self.stats['total_reviews'] > 0:
            success_rate = (self.stats['saved_reviews'] / self.stats['total_reviews']) * 100
            logger.info("📊 Процент успеха: %.1f%%", success_rate)
        logger.info("📄 Лог сохранен в файл: scraper.log")
        logger.info("="*60)


def main():
    base_url = "https://otzovik.com/reviews/sberbank_rossii/"
    
    scraper = OtzovikScraper(
        base_url=base_url,
        output_dir='dataset',
        pages_per_rating=13  # 13 страниц × ~40 отзывов = ~520 отзывов на каждый рейтинг
    )
    
    scraper.run()


if __name__ == "__main__":
    main()