import asyncio
from pyppeteer import launch

async def main() -> None:
    browser = await launch(args=['--no-sandbox'])
    page = await browser.newPage()
    await page.goto('https://github.com/login')
    await page.type('#login_field', 'username')  # your user name here
    await page.type('#password', 'password')  # your password here
    navPromise = asyncio.ensure_future(page.waitForNavigation())
    await page.click('input[type=submit]')
    await navPromise
    cookies = await page.cookies()
    await browser.close()

    browser2 = await launch(args=['--no-sandbox'])
    page2 = await browser2.newPage()
    await page2.setCookie(*cookies)
    await page2.goto('https://github.com/')
    await page2.screenshot({'path': 'github.png'})
    await browser2.close()

asyncio.get_event_loop().run_until_complete(main())