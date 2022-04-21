from django.contrib import admin
from pipelines.models import Pipeline, Operation

admin.site.register(Pipeline)
admin.site.register(Operation)
