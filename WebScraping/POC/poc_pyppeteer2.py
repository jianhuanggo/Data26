import asyncio
from pyppeteer import launch

async def download():
    browser = await launch({"headless": True})
    page = await browser.newPage()
    await page.setExtraHTTPHeaders({'Cookie': 'SetCurrency=EUR; lang=it_IT'})
    await page.goto('http://localhost/test.py')
    await page.setExtraHTTPHeaders({}) # use this if you want to reset extra headers
    html = await page.evaluate('() => document.body.innerHTML')
    await page.close()
    await browser.close()
    return html

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(download())
    print(result)