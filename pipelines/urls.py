from django.urls import path

from pipelines import views

urlpatterns = [
    path('', views.pipelines_view),
    path("pipeline/<int:id>/", views.pipeline_detail_view),
    path("pipeline/create", views.pipeline_create_view),
    path("pipeline/delete/<int:id>/", views.pipeline_delete)
]
