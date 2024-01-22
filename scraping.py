import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import re
from multiprocessing import Process
from db_controller import insert_entry


def scrape_page(base_url: str) -> list[tuple[str, str, int, int, str, str, str, int, str, str]] | bool:
    """Scrapes one page of auto-ria web-site"""
    page_result = []
    response = requests.get(base_url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        lot_elements = soup.find_all('section', class_='ticket-item')
        for lot_element in lot_elements:
            link_element = lot_element.find('a', class_='address')
            if link_element:
                lot_url = link_element.get('href')

                lot_response = requests.get(lot_url)

                if lot_response.status_code == 200:
                    lot_soup = BeautifulSoup(lot_response.text, 'html.parser')
                    try:
                        name = lot_soup.find('h1', class_='head').get('title')
                    except AttributeError:
                        name = None

                    try:
                        price_usd = lot_soup.find('div', class_='price_value').find('strong', class_='').text[:-2]
                        price_usd = int(re.sub(r'[^0-9]', '', price_usd))
                    except AttributeError:
                        price_usd = None

                    try:
                        odometer = lot_soup.find('div', class_='base-information bold').find('span',
                                                                                             class_='size18').text
                        odometer = int(re.sub(r'[^0-9]', '', odometer) + '000')
                    except AttributeError:
                        odometer = None

                    try:
                        username = lot_soup.find('h4', class_='seller_info_name').find('a', href=True,
                                                                                       target='_blank').text
                        username = username.strip()
                    except AttributeError:
                        try:
                            username = lot_soup.find('div',
                                                     class_='seller_info_area').find('div',
                                                                                     class_='seller_info_name').text
                        except AttributeError:
                            username = None
                    try:
                        image_url = lot_soup.find('div', id='photosBlock').find('img',
                                                                                class_='outline m-auto').get('src')
                    except AttributeError:
                        image_url = None

                    try:
                        images_count = lot_soup.find('div', class_='action_disp_all_block').find('a').text
                        images_count = int(re.search(r'\d+', images_count).group())
                    except AttributeError:
                        try:
                            images_count = lot_soup.find('div', class_='count-photo left').find('span',
                                                                                                class_='mhide').text
                            images_count = int(re.search(r'\d+', images_count).group())
                        except AttributeError:
                            images_count = None

                    try:
                        car_number = lot_soup.find('div', class_='t-check').find('span',
                                                                                 class_='state-num ua').text[:10]
                    except AttributeError:
                        try:
                            car_number = lot_soup.find('div', class_='t-check').find('span', class_='state-num ua')
                        except AttributeError:
                            car_number = None

                    try:
                        car_vin = lot_soup.find('div', class_='t-check').find('span',
                                                                              class_=['vin-code', 'label-vin']).text
                    except AttributeError:
                        try:
                            car_vin = lot_soup.find('div', class_='t-check').find('span',
                                                                                  class_=['vin-code', 'label-vin'])
                        except AttributeError:
                            car_vin = None

                    driver = webdriver.Firefox()
                    driver.get(lot_url)
                    button = driver.find_element(By.CLASS_NAME, 'size14')
                    driver.execute_script("arguments[0].click();", button)
                    time.sleep(2)
                    phone_element = driver.find_element(By.CLASS_NAME, 'popup-successful-call-desk')
                    phone_number = phone_element.get_attribute("data-value")
                    phone_number = '+38' + re.sub(r'[^0-9]', '', phone_number)
                    while phone_number == '+38':
                        phone_element = driver.find_element(By.CLASS_NAME, 'popup-successful-call')
                        phone_number = phone_element.get_attribute("data-value")
                        phone_number = '+38' + re.sub(r'[^0-9]', '', phone_number)

                    driver.quit()

                    page_result.append((lot_url, name, price_usd, odometer, username, phone_number, image_url,
                                        images_count, car_number, car_vin))
        return page_result
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return False


def scrape_pages(start_page: int, end_page: int, base_url: str, table: str) -> bool:
    """Scrapes multiple pages within a range and writes data into a db"""
    for page_num in range(start_page, end_page + 1):
        page_url = f"{base_url}?page={page_num}"
        print(f'scraping page {page_num} / {end_page}')
        result = scrape_page(page_url)
        for entry in result:
            insert_entry(entry, table)
    return True


def determine_pages(base_url: str) -> int:
    """Determines total number of auto-ria used cars pages"""
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    total_pages = soup.find('span', class_='page-item dhide text-c').text
    total_pages = int(re.sub(r'[^0-9]', '', total_pages.split('/')[1].strip()))
    return total_pages


def run_scraping_process(base_url: str, total_pages: int, num_processes: int, table: str) -> bool:
    """Runs multiple scrape processes simultaneously"""
    pages_per_process = total_pages // num_processes
    processes = []
    for i in range(num_processes):
        start_page = i * pages_per_process + 1
        end_page = (i + 1) * pages_per_process if i < num_processes - 1 else total_pages
        process = Process(target=scrape_pages, args=(start_page, end_page, base_url, table))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

    return True
