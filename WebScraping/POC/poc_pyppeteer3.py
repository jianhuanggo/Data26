from pyppeteer import launch
import asyncio

#puppeteer-extra.
#2Captcha

extra_headers = {
    'ACCEPT': "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    'ACCEPT-ENCODING': "gzip, deflate, br",
    'ACCEPT-LANGUAGE': "en-US,en;q=0.9",
    'DEVICE-MEMORY': "8",
    'DOWNLINK': "3.35",
    'DPR': "2",
    'ECT': "4g",
    'RTT': "300",
    'SEC-CH-UA': "Not A;Brand;v=99,Chromium;v=90,Google Chrome;v=90",
    'SEC-CH-UA-MOBILE': "?0",
    'SEC-FETCH-DEST': "document",
    'SEC-FETCH-MODE': "navigate",
    'SEC-FETCH-SITE': "none",
    'SEC-FETCH-USER': "?1",
    'UPGRADE-INSECURE-REQUESTS': "1",
    'VIEWPORT-WIDTH': "1440"
}

async def intercept (req):
    print (req.headers)
    await req.continue_ ()
    pass

async def main():
    browser = await launch({"headless": True})
    page = await browser.newPage()
    await page.setRequestInterception(True)
    await page.setExtraHTTPHeaders(extra_headers)
    page.on ('request', lambda req: asyncio.ensure_future (intercept (req)))
    await page.goto('https://www.realtor.com/soldhomeprices/Johns-Creek_GA')
    #html = await page.evaluate('() => document.body.innerHTML')
    html = await page.content()
    await browser.close()
    return html

html=asyncio.get_event_loop().run_until_complete(main())
print(html)