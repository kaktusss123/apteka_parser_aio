import asyncio
from aiohttp import ClientSession
from collections import namedtuple
from lxml.html import fromstring as fs
from csv import DictWriter
from functools import reduce
import logging as log

log.basicConfig(format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s',
                level=log.DEBUG)

Item = namedtuple('Item', ['name', 'count', 'price', 'address'])
TABLE = '//div[@class="module-table"]/table'
NAME = '//h1[@itemprop="name"]/text()'
collected = 0


async def parse_one_vivafarm(url, session, l):
    global collected
    result = []
    log.debug('Parsing {}'.format(url))
    async with session.get(url) as response:
        page = fs(await response.text())
        name = (page.xpath(NAME) or [None])[0]
        table = page.xpath(TABLE)
        for rec in table:
            i = Item(name,
                     (rec.xpath('.//td[@data-label="Кол-во:"]//text()') or [''])[0].strip(),
                     (rec.xpath('.//td[@data-label="Стоимость:"]//text()') or [''])[0].strip(),
                     (rec.xpath('.//td[@data-label="Город:"]//text()') or [''])[0].strip() + ', ' +
                     (rec.xpath('.//td[@data-label="Адрес:"]//text()') or [''])[0].strip())
            result.append(i)
    collected += 1
    log.info('Parsed {}/{}'.format(collected, l))
    return result


async def parse_page_vivafarm(url, session):
    item = r'//a[@class="product-name"]/@href'
    async with session.get(url) as response:
        page = fs(await response.text())
        return page.xpath(item)


async def parse_vivafarm(loop):
    log.info('Starting vivafarm')
    start = r'http://vivafarm.md/124-katalog-all?id_category=124&n=75&p={}'
    last = r'//li[@id="pagination_next_bottom"]/preceding-sibling::li[1]/a/span/text()'
    items = []
    async with ClientSession() as session:
        async with session.get(start.format(1)) as response:
            page_count = int(fs(await response.text()).xpath(last)[0])
            log.info('Collected {} pages'.format(page_count))
        for i in range(1, page_count + 1):
            items += await parse_page_vivafarm(start.format(i), session)
            log.debug('From {} pages collected {} items'.format(i, len(items)))
        log.info('Collecting items finished. Collected {} items'.format(len(items)))
        futures = [parse_one_vivafarm(i, session, len(items)) for i in items]
        log.debug('Futures done')
        write(reduce(lambda a, x: a + x, await asyncio.gather(*futures, loop=loop), []))


def write(items):
    log.info('Writing')
    with open('{}.csv'.format(__file__), 'w', encoding='cp1251', newline='') as f:
        writer = DictWriter(f, Item._fields, delimiter=';')
        writer.writeheader()
        for line in items:
            writer.writerow(line._asdict())


if __name__ == '__main__':
    asyncio.run(parse_vivafarm())
