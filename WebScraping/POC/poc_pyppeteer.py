import asyncio
import pyppeteer
from pyppeteer.network_manager import Request, Response

my_headers = {

}




async def req_intercept(req: Request):
    print(f'Original header: {req.headers}')
    req.headers.update({'Accept-Encoding': 'gzip'})
    await req.continue_(overrides={'headers': req.headers})


async def resp_intercept(resp: Response):
    print(f"New header: {resp.request.headers}")

async def test():
    browser = await pyppeteer.launch()
    page = await browser.newPage()
    await page.setRequestInterception(True)
    page.on('request', req_intercept)
    page.on('response', resp_intercept)
    resp = await page.goto('https://example.org/')
    print(resp.headers)




async def main():
    browser = await pyppeteer.launch()
    page = await browser.newPage()

    ok = await page.goto('https://www.realtor.com/soldhomeprices/Johns-Creek_GA')
    print(ok.headers)
    print("okokok")


    cont = await page.content()
    print(cont)
    await page.screenshot({'/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/WebScraping/POC': 'pyppeteer.png'})
    await browser.close()

asyncio.get_event_loop().run_until_complete(main())
asyncio.get_event_loop().run_until_complete(test())
