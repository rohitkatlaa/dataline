from django.shortcuts import render, redirect
from pipelines.forms import PipelineForm
from pipelines.models import Pipeline
from django.contrib.auth.decorators import login_required

@login_required
def pipelines_view(request):
  pipeline_objs = Pipeline.objects.filter(user__pk=request.user.id)
  context = {
    "pipeline_objs": pipeline_objs,
    "user": request.user
  }

  return render(request, "pipeline/pipeline_view.html", context)


@login_required
def pipeline_detail_view(request, id=None):
  pipeline_obj = None
  if id is not None:
    pipeline_obj = Pipeline.objects.get(id=id)
  context = {
    "object": pipeline_obj
  }

  return render(request, "pipeline/detail.html", context)


@login_required
def pipeline_create_view(request):
  form = PipelineForm(request.POST or None)
  
  if form.is_valid():
    name = form.cleaned_data.get("name")
    pipeline_obj = Pipeline.objects.create(name=name, user=request.user)
    return redirect(pipeline_detail_view, pipeline_obj.id)

  context = {
    "form": form,
  }
  return render(request, "pipeline/create.html", context)


@login_required
def pipeline_delete(request, id=None):
  pipeline_obj = None
  if id is not None:
    pipeline_obj = Pipeline.objects.get(id=id)
    pipeline_obj.delete()
  return redirect(pipelines_view)