
import spacy
from spacy import displacy
nlp = spacy.load('en_core_web_sm')
# Text with nlp
#doc = nlp(" Multiple tornado warnings were issued for parts of New York on Sunday night.The first warning, which expired at 9 p.m., covered the Bronx, Yonkers and New Rochelle. More than 2 million people live in the impacted area.")
# Display Entities
#displacy.render(doc, style="ent")
#for e in doc.ents:
#    print(e.label_, e.text, sep='<--->')

"""
1) Detect format
2) preprocessing to label
3) Based on label, extract info

"""


if __name__ == '__main__':
    a = (r'     <span itemprop="address" itemscope="" itemtype="http://schema.org/PostalAddress">'
        r'<span itemprop="streetAddress">11070 Chandon Way</span>'
        r'<span itemprop="addressLocality">Johns Creek</span>'
        r'<span itemprop="addressRegion">GA</span>'
        r'<span itemprop="postalCode">30097</span> "')

    doc1 = nlp(a)
    for e in doc1.ents:
        print(e.label_, e.text, sep='<--->')