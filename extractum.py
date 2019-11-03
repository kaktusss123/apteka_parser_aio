import asyncio
import logging as log
from csv import DictWriter
from functools import reduce

from aiohttp import ClientSession
from lxml.html import fromstring as fs

from vivafarm import Item

log.basicConfig(format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s',
                level=log.DEBUG)


TABLE = "//section[@class='availability']/table"


async def parse_one_extractum(session, base, url):
    res = []
    async with session.get(base + url) as response:
        page = await response.text()
        page = fs(page)
        table = page.xpath(TABLE)
        for tr in (table or [[]])[0][1:]:
            i = Item((tr.xpath('.//td[1]/text()') or [''])[0].strip(),
                     0,
                     (tr.xpath('./td[@nowrap][2]/text()') or [''])[0].strip(),
                     (tr.xpath('./td/a/text()') or [''])[0].strip())
            res.append(i)
        log.debug(f'{url} done')
    return res


async def parse_extractum():
    log.info(f'Starting {__file__}')
    lnk = "//ul[@class='alphabet fl']/li/a/@href"
    base = 'http://aptekadoktor.com'
    start = 'http://aptekadoktor.com/availability'
    async with ClientSession() as session:
        async with session.get(start) as response:
            page = fs(await response.text())
            urls = page.xpath(lnk)
        log.info(f'Collected {len(urls)} pages')
        futures = [parse_one_extractum(session, base, url) for url in urls]
        write(reduce(lambda a, x: a + x, await asyncio.gather(*futures), []))


def write(items):
    log.info('Writing')
    with open(f'{__file__}.csv', 'w', encoding='cp1251', newline='') as f:
        writer = DictWriter(f, Item._fields, delimiter=';')
        writer.writeheader()
        for line in items:
            writer.writerow(line._asdict())


if __name__ == '__main__':
    asyncio.run(parse_extractum())

