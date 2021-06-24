#https://splinter.readthedocs.io/en/latest/drivers/zope.testbrowser.html
_RESULT="FAIL"
_OLD_CODE = True

headers = { 'Accept':'*/*',
    'Accept-Encoding':'gzip, deflate, sdch',
    'Accept-Language':'en-US,en;q=0.8',
    'Cache-Control':'max-age=0',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'
}
from splinter import Browser
#with Browser('zope.testbrowser', ignore_robots=True) as browser:
#browser = Browser('chrome', headless=True)

browser = Browser(user_agent="Mozilla/5.0 (iPhone; U; CPU like Mac OS X; en)")

#browser = Browser("zope.testbrowser", wait_time=2, headers=headers)
browser.visit("https://www.realtor.com/soldhomeprices/Johns-Creek_GA")
print(browser.status_code.is_success())
print(browser.html)