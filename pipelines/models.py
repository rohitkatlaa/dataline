from django.db import models

# Note: Every time some change is made here, run 
# python manage.py makemigrations 
# python manage.py migrate


class Pipeline(models.Model):
  user = models.ForeignKey("auth.User", blank=True, null=True, on_delete=models.SET_NULL)
  name = models.TextField()