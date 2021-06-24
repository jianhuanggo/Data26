from bs4 import BeautifulSoup

# install chardet or cchardet

"""
</li>, <li class="component_property-card js-component_property-card" data-featured="" data-listingid="2926426171" data-propertyid="6306784848" data-rank="44" data-similar-home-id="similar-home-card" data-url="/realestateandhomes-detail/404-Colonsay-Dr_Duluth_GA_30097_M63067-84848" id="44" itemscope="" itemtype="http://schema.org/SingleFamilyResidence">
<div class="seo-wrap hide">
<span itemprop="address" itemscope="" itemtype="http://schema.org/PostalAddress">
<span itemprop="streetAddress">404 Colonsay Dr</span>
<span itemprop="addressLocality">Johns Creek</span>
<span itemprop="addressRegion">GA</span>
<span itemprop="postalCode">30097</span>
"""


def read_html(filename):
    with open(filename, 'r') as file:
        return file.read()


if __name__ == '__main__':
    html_doc = read_html("/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/WebScraping/POC/realtor.html")
    soup = BeautifulSoup(html_doc, 'html.parser')
    #print(soup)
    a = soup.find_all('li', class_="component_property-card")
    #print(a)
    b = soup('span', itemprop="streetAddress")
    c = soup('span', {'itemprop': ["streetAddress", "addressLocality"]})
    print(c)


