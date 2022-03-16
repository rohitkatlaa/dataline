from django.db import models

# Note: Every time some change is made here, run 
# python manage.py makemigrations 
# python manage.py migrate


class Pipeline(models.Model):
  name = models.TextField()