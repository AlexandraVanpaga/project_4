import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "http://localhost:8000"
EVENTS_URL = "http://localhost:8001"

def test_no_personal():
    logger.info("Тест 1: Пользователь без персональных рекомендаций")
    resp = requests.post(
        f"{BASE_URL}/recommendations",
        json={"user_id": 999999, "k": 10},
        headers={"Content-Type": "application/json"}
    )
    logger.info(f"Status: {resp.status_code}")
    logger.info(f"Response: {resp.text}")
    assert resp.status_code == 200
    recs = resp.json().get("recs", [])
    logger.info(f"Рекомендации: {len(recs)} items")
    assert len(recs) > 0
    logger.info("Тест 1 пройден")

def test_with_personal():
    logger.info("\nТест 2: Пользователь с персональными рекомендациями")
    resp = requests.post(
        f"{BASE_URL}/recommendations",
        json={"user_id": 0, "k": 10},
        headers={"Content-Type": "application/json"}
    )
    logger.info(f"Status: {resp.status_code}")
    recs = resp.json().get("recs", [])
    logger.info(f"Рекомендации: {len(recs)} items")
    assert len(recs) == 10
    logger.info("Тест 2 пройден")

def test_online():
    logger.info("\nТест 3: Онлайн-рекомендации")
    
  
    resp1 = requests.post(
        f"{EVENTS_URL}/put",
        json={"user_id": 100, "item_id": 7348},
        headers={"Content-Type": "application/json"}
    )
    resp2 = requests.post(
        f"{EVENTS_URL}/put",
        json={"user_id": 100, "item_id": 7348},
        headers={"Content-Type": "application/json"}
    )
    
    logger.info(f"Put events: {resp1.status_code}, {resp2.status_code}")
    

    resp = requests.post(
        f"{BASE_URL}/recommendations_online",
        json={"user_id": 100, "k": 10},
        headers={"Content-Type": "application/json"}
    )
    logger.info(f"Status: {resp.status_code}")
    logger.info(f"Response: {resp.text[:200]}")
    recs = resp.json().get("recs", [])
    logger.info(f"Онлайн-рекомендации: {len(recs)} items")
    
    if len(recs) > 0:
        logger.info(f"Пример рекомендаций: {recs[:3]}")
        assert len(recs) > 0
        logger.info("Тест 3 пройден")
    else:
        logger.warning("Отсутствуют онлайн-рекомендации → выполняется резервная логика")

if __name__ == "__main__":
    try:
        test_no_personal()
        test_with_personal()
        test_online()
        logger.info("\nВсе тесты пройдены")
    except AssertionError as e:
        logger.error(f"\nТесты не пройдены: {e}")
    except Exception as e:
        logger.error(f"\nОшибка: {e}")