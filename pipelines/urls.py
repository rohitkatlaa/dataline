from django.urls import path

from pipelines import views

urlpatterns = [
    path('', views.home_view, name='home'),
    path('pipelines/', views.pipelines_view, name='list'),
    path("pipeline/<int:id>/", views.pipeline_detail_view, name='detail'),
    path("pipeline/create", views.pipeline_create_view, name='create'),
    path("pipeline/update/<int:id>/", views.pipeline_update_view, name='update'),
    path("pipeline/delete/<int:id>/", views.pipeline_delete, name='delete'),
    path("pipeline/operation/delete/<int:id>/", views.operation_delete_view, name='delete_operation')
]