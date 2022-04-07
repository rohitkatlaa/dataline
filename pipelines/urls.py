from django.urls import path
from django.conf import settings
from django.conf.urls.static import static
from pipelines import views

urlpatterns = [
    path('', views.home_view, name='list'),
    path('pipelines/', views.pipelines_view, name='list'),
    path("pipeline/<int:id>/", views.pipeline_detail_view, name='detail'),
    path("pipeline/execute/<int:id>/", views.pipeline_execute_view, name='execute_pipeline'),
    path("pipeline/execution/output/<int:id>/<str:filename>", views.pipeline_execution_output_view, name='pipeline_execution_output'),
    path("pipeline/create", views.pipeline_create_view, name='create'),
    path("pipeline/update/<int:id>/", views.pipeline_update_view, name='update'),
    path("pipeline/delete/<int:id>/", views.pipeline_delete, name='delete'),
    path("pipeline/operation/delete/<int:id>/", views.operation_delete_view, name='delete_operation'),
    path("pipeline/output_file/<str:name>/", views.send_output_file, name='output_file')
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)