from django.test import SimpleTestCase
from django.urls import reverse, resolve
from pipelines.views import *

class TestUrls(SimpleTestCase):

    def test_home_view_url_is_resolved(self):
        url = reverse('home')
        self.assertEquals(resolve(url).func, home_view)

    def test_pipelines_view_url_is_resolved(self):
        url = reverse('list')
        self.assertEquals(resolve(url).func, pipelines_view)

    def test_pipeline_detail_view_url_is_resolved(self):
        url = reverse('detail', args=["1"])
        self.assertEquals(resolve(url).func, pipeline_detail_view)

    def test_pipeline_create_view_url_is_resolved(self):
        url = reverse('create')
        self.assertEquals(resolve(url).func, pipeline_create_view)

    def test_pipeline_update_view_url_is_resolved(self):
        url = reverse('update', args=["1"])
        self.assertEquals(resolve(url).func, pipeline_update_view)

    def test_pipeline_delete_url_is_resolved(self):
        url = reverse('delete', args=["1"])
        self.assertEquals(resolve(url).func, pipeline_delete)

    def test_operation_delete_view_url_is_resolved(self):
        url = reverse('delete_operation', args=["1"])
        self.assertEquals(resolve(url).func, operation_delete_view)