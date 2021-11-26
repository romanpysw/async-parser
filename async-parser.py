import time
import csv
import asyncio
import aiohttp
from bs4 import BeautifulSoup as BS

urls = ["https://apteka.ru/sym/leka/kost/?page=",
        "https://apteka.ru/sym/leka/gorm/?page=",
        "https://apteka.ru/sym/leka/moche/?page=",
        "https://apteka.ru/sym/leka/proti/?page=",
        "https://apteka.ru/sym/leka/derm/?page=",
        "https://apteka.ru/sym/leka/nerv/?page=",
        "https://apteka.ru/sym/leka/protiv/?page=",
        "https://apteka.ru/sym/leka/diha/?page=",
        "https://apteka.ru/sym/leka/pishe/?page=",
        "https://apteka.ru/sym/leka/proch/?page=",
        "https://apteka.ru/sym/leka/prep/?page=",
        "https://apteka.ru/sym/leka/krov/?page=",
        "https://apteka.ru/sym/leka/serd/?page=",
        "https://apteka.ru/sym/leka/prot/?page="
        ]

names = ["Товар", "Цена", "Производитель", "Ссылка"]
list_name = ".cards-list > .catalog-card"
selectors = [".catalog-card__name", ".moneyprice__roubles", ".catalog-card__vendor", "url"]



async def scrap_chapter(session, url_list, names_csv, web_list_name, web_selectors):
    if not (len(names_csv) == len(web_selectors)):
        return "[ERR] can't match names and selectors!"

    wFile = open("async-res.csv", mode = "a", encoding = 'utf-8')
    file_writer = csv.DictWriter(wFile, delimiter = ';', lineterminator = '\n', fieldnames = names_csv)
    meta_field = dict.fromkeys(names_csv)
    cur_url = 0
    count_page = 0

    while(1):
        count_page += 1
        async with session.get((url_list + str(count_page))) as response:
            response_text = await response.text()
            html = BS(response_text, 'html.parser')
            item_list = html.select(web_list_name)
            print("----------" + str(count_page) + "----------")

        if len(item_list):
            for item in item_list:
                i = 0
                for cur_selector in web_selectors:
                    if web_selectors[i] == "url":
                        meta_data = item.find('a', href = True)
                        meta_field[names_csv[i]] = meta_data.get('href')
                        i += 1
                        continue

                    meta_data = item.select(web_selectors[i])
                    if not meta_data:
                        meta_field[names_csv[i]] = "NoData"
                    else:
                        meta_field[names_csv[i]] = meta_data[0].text
                    i += 1
                file_writer.writerow(meta_field)
                meta_field.clear()
                meta_field = dict.fromkeys(names_csv)
        else:
            break
    return "[OK] Parsing successed!"

async def gather_data():
    async with aiohttp.ClientSession() as session:
        tasks = [ asyncio.create_task(scrap_chapter(session, urls[i-1], names, list_name, selectors)) for i in range( 1, len(urls) + 1 )  ]
        await asyncio.gather(*tasks)


def main():
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(gather_data())

if __name__ == "__main__":
    start_time = time.time()
    main()
    print("By " + str(time.time() - start_time) + " seconds")
