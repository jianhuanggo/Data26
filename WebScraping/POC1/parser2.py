import usaddress

if __name__ == '__main__':
    a = (r'     <span itemprop="address" itemscope="" itemtype="http://schema.org/PostalAddress">'
        r'<span itemprop="streetAddress">11070 Chandon Way</span>'
        r'<span itemprop="addressLocality">Johns Creek</span>'
        r'<span itemprop="addressRegion">GA</span>'
        r'<span itemprop="postalCode">30097</span> "')

print(usaddress.parse(a))
