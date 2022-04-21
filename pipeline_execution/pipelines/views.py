from django.shortcuts import get_object_or_404, render, redirect
from django.http import FileResponse
from pipeline_execution.settings import BASE_DIR
from pipelines.models import Pipeline
from django.contrib.auth.decorators import login_required
from django.core.files.storage import FileSystemStorage
import pipelines.pipeline_execution.data as pdata
import pipelines.pipeline_execution.pipeline_structure as ps
import os

def pipeline_execute_view(request, id=None):
  pipeline_obj = get_object_or_404(Pipeline, id=id)
  context = {
    "object": pipeline_obj,
    "dataline_url": os.environ.get("DATALINE_URL1"),
  }
  if request.method == 'POST' and request.FILES['data_file']:
    data_file = request.FILES['data_file']
    fs = FileSystemStorage()
    filename = fs.save(data_file.name, data_file)
    uploaded_file_url = fs.url(filename)
    return redirect(pipeline_execution_output_view, id, filename)
  return render(request, "pipeline/execute.html", context)


def pipeline_execution_output_view(request, id=None, filename=None):
  pipeline_obj = get_object_or_404(Pipeline, id=id)

  try:
    input_data_dict = pdata.DataReader.generate_csv_data_dict(os.path.join(BASE_DIR, 'media', filename), filename.split(".")[0])

    p_format = ps.PipelineFormat()
    stage_dict = {}
    for operation in pipeline_obj.get_operation_children():
      if operation.stage_name not in stage_dict.keys():
        s_format = ps.StageFormat(operation.stage_name)
        stage_dict[operation.stage_name] = s_format
      
      t_format = ps.TaskFormat([operation.data_input_name], operation.operation_name, operation.parameters, operation.data_output_name)

      stage_dict[operation.stage_name].add_task_format(t_format)


    for stage_name in stage_dict:
      p_format.add_stage_format(stage_dict[stage_name])

    pipeline_instance = ps.Pipeline(pipeline_obj.name, input_data_dict, p_format)
    output = pipeline_instance.execute()
    final_output = output.fetch_dt(operation.data_output_name).fetch_table()
    output_filename = os.path.join(BASE_DIR, 'media', pipeline_instance.name+"_output.csv")
    final_output.to_csv(output_filename)
    os.remove(os.path.join(BASE_DIR, 'media', filename))
    output = pipeline_instance.name+"_output.csv"
    status = "success"
  except Exception as e:
    os.remove( os.path.join(BASE_DIR, 'media', filename))
    print(e)
    status = "failure"
    output = e

  context = {
    "object": pipeline_obj,
    "status": status,
    "output": output,
    "dataline_url": os.environ.get("DATALINE_URL1"),
  }
  return render(request, "pipeline/execution_output.html", context)

login_required
def send_output_file(request, name):
  filename = os.path.join(BASE_DIR, 'media', name)
  output = open(filename, 'rb')
  response = FileResponse(output)
  return response