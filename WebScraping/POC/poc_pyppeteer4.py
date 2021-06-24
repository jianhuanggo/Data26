import asyncio
import time
from pyppeteer import launch
from pyppeteer_stealth import stealth

_RESULT="SUCCESS"
#https://javascriptwebscrapingguy.com/avoid-being-blocked-with-puppeteer/
#Best implementation: JavaScript

async def main(url):
    browser = await launch(headless=True)
    page = await browser.newPage()
    page.setDefaultNavigationTimeout(0)

    await stealth(page)  # <-- Here
    await page.goto(url)
    #await page.screenshot({"path": "realtor.png", "fullPage": True})
    html = await page.content()
    await browser.close()
    return html

if __name__ == '__main__':
    _url = "https://www.realtor.com/soldhomeprices/Suwanee_GA"
    for i in range(11, 12):
        url = f"{_url}/pg-{i}" if i >= 2 else _url
        html=asyncio.get_event_loop().run_until_complete(main(url))
        print(f"got data from {url}")
        time.sleep(2)
        with open(f"Suwanee-realtor-{i}.html", 'w') as file:
            file.write(html)

