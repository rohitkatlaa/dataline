import imp
from django.test import TestCase, Client
from django.urls import reverse
from pipelines.models import *
from django.contrib.auth.models import User
import json

class TestViews(TestCase):

    def setUp(self):
        self.client = Client()
        self.user = User.objects.create_user(username='tarun1', password='Tarun@123')
        self.home_url = reverse('home')
        self.list_url = reverse('list')
        self.detail_url = reverse('detail', args=["1"])
        self.execute_pipeline_url = reverse('execute_pipeline', args=["1"])
        self.pipeline_execution_output_url = reverse('pipeline_execution_output', args=["1", "tarun"])
        self.create_url = reverse('create')
        self.update_url = reverse('update', args=["1"])
        self.delete_url = reverse('delete', args=["1"])
        self.delete_operation_url = reverse('delete_operation', args=["1"])
        self.output_file_url = reverse('output_file', args=["tarun"])

    def test_home_view_GET(self):
        response = self.client.get(self.home_url)

        self.assertEquals(response.status_code, 200)
        self.assertTemplateUsed(response, 'home.html')

    def test_pipelines_view_GET(self):
        self.client.login(username='tarun1', password='Tarun@123')
        response = self.client.get(self.list_url)

        self.assertEquals(response.status_code, 200)
        self.assertTemplateUsed(response, 'pipeline/pipeline_view.html')

    def test_pipeline_detail_view_GET(self):
        self.client.login(username='tarun1', password='Tarun@123')
        response = self.client.get(self.detail_url)

        self.assertEquals(response.status_code, 200)
        self.assertTemplateUsed(response, 'pipeline/detail.html')

    # def test_pipeline_execute_view_GET(self):
    #     self.client.login(username='tarun1', password='Tarun@123')
    #     response = self.client.get(self.execute_pipeline_url)

    #     self.assertEquals(response.status_code, 200)
    #     self.assertTemplateUsed(response, 'pipeline/execute.html')

    # def test_pipeline_execution_output_view_GET(self):
    #     self.client.login(username='tarun1', password='Tarun@123')
    #     response = self.client.get(self.pipeline_execution_output_url)

    #     self.assertEquals(response.status_code, 200)
    #     self.assertTemplateUsed(response, 'pipeline/execution_output.html')

    # def test_pipeline_create_view_GET(self):
    #     self.client.login(username='tarun1', password='Tarun@123')
    #     response = self.client.get(self.create_url)

    #     self.assertEquals(response.status_code, 200)
    #     self.assertTemplateUsed(response, 'pipeline/create-update.html')

    # def test_pipeline_update_view_GET(self):
    #     self.client.login(username='tarun1', password='Tarun@123')
    #     response = self.client.get(self.update_url)

    #     self.assertEquals(response.status_code, 200)
    #     self.assertTemplateUsed(response, 'pipeline/create-update.html')

    # def test_pipeline_delete_GET(self):
    #     response = self.client.get(self.delete_url)

    #     self.assertEquals(response.status_code, 200)
    #     self.assertTemplateUsed(response, 'pipeline/delete.html')

    # def test_operation_delete_view_GET(self):
    #     response = self.client.get(self.delete_operation_url)

    #     self.assertEquals(response.status_code, 200)
    #     self.assertTemplateUsed(response, 'pipeline/delete.html')

    # def test_send_output_file_GET(self):
    #     response = self.client.get(self.output_file_url)

    #     self.assertEquals(response.status_code, 200)
    #     self.assertTemplateUsed(response, 'home.html')