import os
import time
import requests
from bs4 import BeautifulSoup

def create_directory_structure():
    base_dir = 'dataset'
    
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
        print(f"✓ Создана папка: {base_dir}")
    
    for rating in range(1, 6):
        rating_dir = os.path.join(base_dir, str(rating))
        if not os.path.exists(rating_dir):
            os.makedirs(rating_dir)
            print(f"✓ Создана папка: {rating_dir}")
    
    print("✓ Структура папок создана успешно")

def fetch_page_with_requests(url):
    print(f"\nПолучение страницы: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3'
    }
    
    try:
        print("⏳ Ожидание 10 секунд перед подключением...")
        time.sleep(10)
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        print("✓ Страница успешно загружена")
        return response.text
    
    except requests.exceptions.RequestException as e:
        print(f"✗ Ошибка при загрузке страницы: {e}")
        return None

def parse_reviews(html):
    if not html:
        print("✗ Нет данных для парсинга")
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    reviews = []
    
    # Ищем все блоки отзывов по классу 'item' и проверяем наличие статусов
    all_items = soup.find_all('div', class_='item')
    
    # Фильтруем только те, что содержат классы статуса (отзывы)
    review_blocks = [item for item in all_items if 'status4' in item.get('class', []) or 'status10' in item.get('class', [])]
    
    print(f"\nНайдено отзывов на странице: {len(review_blocks)}")
    
    for block in review_blocks[:10]:
        try:
            # Получаем рейтинг из meta тега
            rating_meta = block.find('meta', itemprop='reviewRating')
            if rating_meta and rating_meta.get('content'):
                rating = int(float(rating_meta['content']))
            else:
                # Альтернативный способ - из видимого элемента
                rating_elem = block.find('div', class_='rating-score')
                if rating_elem:
                    rating_text = rating_elem.get_text(strip=True)
                    rating = int(''.join(filter(str.isdigit, rating_text))) if rating_text else None
                else:
                    rating = None
            
            # Получаем заголовок отзыва
            title_elem = block.find('a', class_='review-title')
            title = title_elem.get_text(strip=True) if title_elem else "Без названия"
            
            # Получаем краткий текст отзыва
            teaser_elem = block.find('div', class_='review-teaser')
            teaser = teaser_elem.get_text(strip=True) if teaser_elem else "Текст отзыва отсутствует"
            
            # Получаем дату отзыва
            date_elem = block.find('div', class_='review-postdate')
            date = date_elem.get_text(strip=True) if date_elem else "Дата не указана"
            
            # Получаем автора
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
            print(f"  ✓ Отзыв: {title[:50]}... (Рейтинг: {rating}, Автор: {author})")
            
        except Exception as e:
            print(f"  ✗ Ошибка при парсинге отзыва: {e}")
            continue
    
    return reviews

def save_review_to_file(review, rating, review_number):
    if not rating or rating < 1 or rating > 5:
        print(f"  ⚠ Пропущен отзыв с некорректным рейтингом: {rating}")
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
        
        print(f"  ✓ Сохранен: {filepath}")
        return True
    
    except Exception as e:
        print(f"  ✗ Ошибка при сохранении файла {filepath}: {e}")
        return False

def main():
    print("="*60)
    print("Web-Scraper для otzovik.com")
    print("Парсинг отзывов о Сбербанке")
    print("="*60)
    
    create_directory_structure()
    
    base_url = "https://otzovik.com/reviews/sberbank_rossii/"
    
    print(f"\n🎯 Целевой сайт: {base_url}")
    
    html = fetch_page_with_requests(base_url)
    
    if not html:
        print("✗ Не удалось получить данные с сайта")
        return
    
    reviews = parse_reviews(html)
    
    if not reviews:
        print("✗ Отзывы не найдены")
        return
    
    print(f"\n📊 Всего найдено отзывов: {len(reviews)}")
    
    saved_count = 0
    for i, review in enumerate(reviews, start=1):
        if save_review_to_file(review, review['rating'], i):
            saved_count += 1
    
    print(f"\n{'='*60}")
    print(f"✅ Завершено! Сохранено {saved_count} отзывов")
    print(f"📁 Файлы сохранены в папку: dataset/")
    print("="*60)

if __name__ == "__main__":
    main()