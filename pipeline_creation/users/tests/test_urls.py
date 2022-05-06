from django.test import SimpleTestCase
from django.urls import reverse, resolve
from users.views import *

class TestUrls(SimpleTestCase):

    def test_home_view_url_is_resolved(self):
        url = reverse('login')
        self.assertEquals(resolve(url).func, login_view)

    def test_pipelines_view_url_is_resolved(self):
        url = reverse('logout')
        self.assertEquals(resolve(url).func, logout_view)

    def test_pipeline_detail_view_url_is_resolved(self):
        url = reverse('register')
        self.assertEquals(resolve(url).func, register_view)
