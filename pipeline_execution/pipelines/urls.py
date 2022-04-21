from django.urls import path
from django.conf import settings
from django.conf.urls.static import static
from pipelines import views

urlpatterns = [
    path("pipeline/execute/<int:id>/", views.pipeline_execute_view, name='execute_pipeline'),
    path("pipeline/execution/output/<int:id>/<str:filename>", views.pipeline_execution_output_view, name='pipeline_execution_output'),
    path("pipeline/output_file/<str:name>/", views.send_output_file, name='output_file')
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)