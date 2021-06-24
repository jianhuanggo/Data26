from requests_html import HTMLSession
from requests_html import AsyncHTMLSession
asession = AsyncHTMLSession()


class PGRequestHtml():

    def __init__(self):
        self._html_session = None
        self._async_html_session = None

    def get_html_session(self):
        if self._html_session:
            return self._html_session
        else:
            self._html_session = HTMLSession()
        return self._html_session

    def async_get_html_session(self):
        if self._async_html_session:
            return self._async_html_session
        else:
            self._async_html_session = AsyncHTMLSession()
        return self._async_html_session

    def get_url(self, url):
        return self.get_html_session().get(url)

    """
    def async_get_url(self, url):
        return await self.async_get_html_session().get(url)

    def async_run(self):
        async def get_pythonorg():
            r = await asession.get('https://python.org/')

        async def get_reddit():
            r = await asession.get('https://reddit.com/')

        async def get_google():
            r = await asession.get('https://google.com/')

        session.run(get_pythonorg, get_reddit, get_google)
    
    """

    def get_xpath(self, url, selection):
        return self.get_url(url).html.xpath(selection)

    def find_css(self, url, selection: str):
        return self.get_url(url).html.find(selection)


if __name__ == '__main__':
    Test = PGRequestHtml()
    s = HTMLSession()
    r = s.get("https://www.publix.com/covid-vaccine/georgia")
    r.html.render(sleep=1)
    print(r.html.links)
    sel = 'tbody[id=counties]'
    #sel = 'img[src]'
    #.full_text)
    print(r.html.find(sel, first=True).full_text)
    #print(r.html.xpath(sel))
    exit(0)

    print(Test.find_css("https://www.publix.com/covid-vaccine/georgia", "body.County.availability"))



#r.html.xpath('a')



#s = HTMLSession()
#r = s.get("")

"""
sel = "body.default"    Find text "default" in the web page
sel = "a[href]"         Find all links in the web page
sel = "#default"        Find id = default in the web page
sel = ".default"        Find class = default in the web page
sel = "img.thumnail"    Find image in the web page
sel = "img[src]"        Find image in the web page

"""


