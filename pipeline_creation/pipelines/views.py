from django.shortcuts import get_object_or_404, render, redirect
from django.forms.models import modelformset_factory
from pipelines.forms import OperationForm, PipelineForm
from pipelines.models import Pipeline, Operation
from django.contrib.auth.decorators import login_required
import os

def home_view(request):
  context = {
    "user": request.user
  }

  return render(request, "home.html", context)


def pipelines_view(request):
  all_pipeline_objs = Pipeline.objects.all()
  my_pipeline_objs = Pipeline.objects.filter(user__pk=request.user.id)
  context = {
    "all_pipeline_objs": all_pipeline_objs,
    "my_pipeline_objs": my_pipeline_objs,
    "user": request.user
  }

  return render(request, "pipeline/pipeline_view.html", context)


def pipeline_detail_view(request, id=None):
  pipeline_obj = get_object_or_404(Pipeline, id=id)
  context = {
    "object": pipeline_obj,
    "is_owner": pipeline_obj.user == request.user,
    "dataline_url": os.environ.get("DATALINE_URL2"),
  }

  return render(request, "pipeline/detail.html", context)


@login_required
def pipeline_create_view(request):
  form = PipelineForm(request.POST or None)
  OperationFormset = modelformset_factory(Operation, form=OperationForm, extra=0)
  operation_qs = Operation.objects.none()
  operation_formset = OperationFormset(request.POST or None, queryset=operation_qs)

  context = {
    "form": form,
    "formset": operation_formset
  }
  
  if all([form.is_valid(), operation_formset.is_valid()]):
    pipeline = form.save(commit=False)
    pipeline.user = request.user
    pipeline.save()
    for form in operation_formset:
      operation = form.save(commit=False)
      operation.pipeline = pipeline
      operation.save()
    return redirect(pipeline_detail_view, pipeline.id)

  return render(request, "pipeline/create-update.html", context)


@login_required
def pipeline_update_view(request, id=None):
  pipeline_obj = get_object_or_404(Pipeline, id=id, user=request.user)

  form = PipelineForm(request.POST or None, instance=pipeline_obj)

  OperationFormset = modelformset_factory(Operation, form=OperationForm, extra=0)
  operation_qs = pipeline_obj.operation_set.all()
  operation_formset = OperationFormset(request.POST or None, queryset=operation_qs)

  context = {
    "form": form,
    "formset": operation_formset
  }
  if all([form.is_valid(), operation_formset.is_valid()]):
    pipeline = form.save(commit=False)
    pipeline.save()
    for form in operation_formset:
      operation = form.save(commit=False)
      operation.pipeline = pipeline
      operation.save()
    return redirect(pipeline_detail_view, pipeline_obj.id)

  return render(request, "pipeline/create-update.html", context)


@login_required
def pipeline_delete(request, id=None):
  pipeline_obj = Pipeline.objects.get(id=id)
  if request.method == "POST":
    pipeline_obj.delete()
    return redirect(pipelines_view)
  context = {
    "obj_name": "Pipeline",
    "name": pipeline_obj.name
  }
  return render(request, "pipeline/delete.html", context)


@login_required
def operation_delete_view(request, id=None):
  obj = Operation.objects.get(id=id, pipeline__user=request.user)
  parent_id = obj.pipeline.id
  if request.method == "POST":
    obj.delete()
    return redirect(pipeline_detail_view, parent_id)
  context = {
    "obj_name": "Operation",
    "name": obj.operation_name
  }
  return render(request, "pipeline/delete.html", context)

