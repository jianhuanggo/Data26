import requests
from requests import utils

_RESULT="WIP"

__title__ = 'MechanicalSoup'
__description__ = 'A Python library for automating interaction with websites'
__url__ = 'https://mechanicalsoup.readthedocs.io/'
__github_url__ = 'https://github.com/MechanicalSoup/MechanicalSoup'
__version__ = '1.1.0-dev'
__license__ = 'MIT'
__github_assets_absoluteURL__ = """\
https://raw.githubusercontent.com/MechanicalSoup/MechanicalSoup/master"""

session = requests.session()
requests_ua = utils.default_user_agent()
user_agent = f'{requests_ua} ({__title__}/{__version__})'
session.headers['User-agent'] = user_agent

response = session.request(*args, **kwargs)


def _request(self, form, url=None, **kwargs):
    """Extract input data from the form to pass to a Requests session."""
    request_kwargs = Browser.get_request_kwargs(form, url, **kwargs)
    return self.session.request(**request_kwargs)

def set_user_agent(self, user_agent):
    """Replaces the current user agent in the requests session headers."""
    # set a default user_agent if not specified
    if user_agent is None:
        requests_ua = requests.utils.default_user_agent()
        user_agent = f'{requests_ua} ({__title__}/{__version__})'

    # the requests module uses a case-insensitive dict for session headers
    self.session.headers['User-agent'] = user_agent


def request(self, *args, **kwargs):
    """Straightforward wrapper around `requests.Session.request
    <http://docs.python-requests.org/en/master/api/#requests.Session.request>`__.
    :return: `requests.Response
        <http://docs.python-requests.org/en/master/api/#requests.Response>`__
        object with a *soup*-attribute added by :func:`add_soup`.
    This is a low-level function that should not be called for
    basic usage (use :func:`get` or :func:`post` instead). Use it if you
    need an HTTP verb that MechanicalSoup doesn't manage (e.g. MKCOL) for
    example.
    """
    response = self.session.request(*args, **kwargs)
    Browser.add_soup(response, self.soup_config)
    return response