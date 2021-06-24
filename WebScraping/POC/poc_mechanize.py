import re
import mechanize

_RESULT="FAIL"

br = mechanize.Browser()
br.open("https://www.realtor.com/soldhomeprices/Johns-Creek_GA")
# follow second link with element text matching regular expression
#response1 = br.follow_link(text_regex=r"cheese\s*shop", nr=1)
print(br.viewing_html())