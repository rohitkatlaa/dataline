from django.conf import settings
from django.db import models
from joblib import parallel_backend

# Note: Every time some change is made here, run 
# python manage.py makemigrations 
# python manage.py migrate


class Pipeline(models.Model):
  user = models.ForeignKey("auth.User", blank=True, null=True, on_delete=models.SET_NULL)
  name = models.TextField()
  pipeline_description = models.TextField()
  timestamp = models.DateTimeField(auto_now_add=True)
  updated = models.DateTimeField(auto_now=True)

  def get_operation_children(self):
    return self.operation_set.all() 


class Operation(models.Model):
  pipeline = models.ForeignKey(Pipeline, blank=True, null=True, on_delete=models.SET_NULL)
  stage_name = models.TextField()
  operation_name = models.TextField()
  column_name = models.TextField()
  data_input_name = models.TextField()
  data_output_name = models.TextField()
  timestamp = models.DateTimeField(auto_now_add=True)
  updated = models.DateTimeField(auto_now=True)
  parameters = models.JSONField()