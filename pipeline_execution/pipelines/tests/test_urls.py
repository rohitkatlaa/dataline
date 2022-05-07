from django.test import SimpleTestCase
from django.urls import reverse, resolve
from pipelines.views import *

class TestUrls(SimpleTestCase):

    def test_pipeline_execute_view_url_is_resolved(self):
        url = reverse('execute_pipeline', args=["1"])
        self.assertEquals(resolve(url).func, pipeline_execute_view)

    def test_pipeline_execution_output_view_url_is_resolved(self):
        url = reverse('pipeline_execution_output', args=["1", "tarun"])
        self.assertEquals(resolve(url).func, pipeline_execution_output_view)

    def test_send_output_file_url_is_resolved(self):
        url = reverse('output_file', args=["tarun"])
        self.assertEquals(resolve(url).func, send_output_file)
