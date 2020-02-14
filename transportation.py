import csv
import re
import time
from contextlib import closing

import aiohttp
from aiohttp import ClientSession
import asyncio

import xlrd
from bs4 import BeautifulSoup

URL_PATTERN = 'https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=USDOT&query_string={0}'


async def fetch_page(session, dot):
    url = URL_PATTERN.format(dot)
    async with session.get(url) as response:
        assert response.status == 200
        return await response.text()


def crawl_usdot_data(dots):
    print("Fetching pages")
    pages = asyncio.run(get_pages(dots))

    print("Processing content")
    result = []
    for page in pages:
        result.append(process_data(page))
    return result


async def get_pages(dots):
    tasks = []
    pages = []
    start = time.perf_counter()
    async with aiohttp.ClientSession() as session:
        for dot in dots:
            tasks.append(fetch_page(session, dot))
        pages = await asyncio.gather(*tasks)
    duration = time.perf_counter() - start
    msg = 'It took {:4.2f} seconds.'
    print(msg.format(duration))
    return pages


def process_data(content):
    bs = BeautifulSoup(content, 'html.parser')
    bold_txt = bs.find_all('b')
    for b in bold_txt:
        try:
            re_result = re.search(
                'The information below reflects the content of the FMCSA management information systems as of (\d{2}\/\d{2}\/\d{4})',
                b.get_text(strip=True, separator=' '))
            if re_result:
                date = re_result.group(1).strip()
                break
        except AttributeError:
            print('Error')
    info = bs.find('center').get_text(strip=True, separator=' ')
    result_dict = {'Date': date}
    operating = re.search('Operating Status: (.*) Out', info).group(1).strip()
    result_dict['Operating Status'] = operating
    legal_name = re.search('Legal Name: (.*)DBA', info).group(1).strip()
    result_dict['Legal Name'] = legal_name
    dba_name = re.search('DBA Name: (.*)Physical', info).group(1).strip()
    result_dict['DBA Name'] = dba_name
    physical_address = re.search('Physical Address: (.*)Phone:', info).group(1).strip()
    result_dict['Physical Address'] = physical_address
    mailing_address = re.search('Mailing Address: (.*)USDOT', info).group(1).strip()
    result_dict['Mailing Address'] = mailing_address
    usdot_number = re.search('USDOT Number: (\d+) State', info).group(1).strip()
    result_dict['USDOT Number'] = usdot_number
    power_units = re.search('Power Units: (.*)Drivers', info).group(1).strip()
    result_dict['Power Units'] = power_units
    drivers = re.search('Drivers: (\d+) MCS-150', info).group(1).strip()
    result_dict['Drivers'] = drivers
    return result_dict


def write_csv(data_to_store):
    with open('USDOT_{0:d}.csv'.format(int(time.time())), mode='w', newline='', encoding="utf-8") as csv_file:
        fieldnames = data_to_store[0].keys()
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in data_to_store:
            writer.writerow(row)


def get_dot_values(excel_file_name):
    wb = xlrd.open_workbook(excel_file_name)
    sheet = wb.sheet_by_index(0)
    dot_float_values = sheet.col_values(0)[1:]
    return list(map(lambda d: int(d), dot_float_values))


if __name__ == '__main__':

    def main():

        # Retrieve all target USDOT values
        print("Read XLS to get USDOS values")
        excel_file_name = "dots.xlsx"
        dots = get_dot_values(excel_file_name)
        print('Retrieved: {0} values'.format(len(dots)))

        # Process all USDOT and fetch required data from corespondent web page
        result_dots_data = crawl_usdot_data(dots)

        # Store results
        write_csv(result_dots_data)

    main()

