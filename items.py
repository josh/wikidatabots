from pywikibot import ItemPage

from wikidata import SITE

CRITIC_REVIEW_QID = "Q80698083"
OPENCRITIC_QID = "Q21039459"
REDIRECT_QID = "Q45403344"
WITHDRAWN_IDENTIFIER_VALUE_QID = "Q21441764"

CRITIC_REVIEW_ITEM = ItemPage(SITE, CRITIC_REVIEW_QID)
OPENCRITIC_ITEM = ItemPage(SITE, OPENCRITIC_QID)
REDIRECT_ITEM = ItemPage(SITE, REDIRECT_QID)
WITHDRAWN_IDENTIFIER_VALUE_ITEM = ItemPage(SITE, WITHDRAWN_IDENTIFIER_VALUE_QID)
