import pipelines.pipeline_execution.data as data
import pipelines.pipeline_execution.operations as operations
from typing import List


OPERATIONS_MAPPING = {
  "Lower Case": operations.LowerCaseOperation,
  "Upper Case": operations.UpperCaseOperation,
  "Add Value": operations.AddValueOperation,
  "Multiply Value": operations.MultiplyValueOperation,
  "Divide Value": operations.DivideValueOperation,
  "Add Columns": operations.AddColumnsOperation,
  "Multiply Columns": operations.MultiplyColumnsOperation,
  "Divide Columns": operations.DivideColumnsOperation,
  "Length of Text": operations.LengthOfTextOperation,
  "Date Difference": operations.DateDiffOperation,
  "Add Date": operations.AddDateOperation,
  "Split Column by delimiter": operations.SplitColumnOperation,
  "Join Tables": operations.JoinTableOperation,
  "Rename Column": operations.RenameColumnOperation,
  "Delete Column": operations.DeleteColumnOperation,
  "Integer Filter": operations.IntFilterOperation,
  "Delete Duplicate rows": operations.DeleteDuplicatesOperation,
  "RegEx filter": operations.RegExFilter,
  "Add Variable": operations.AddVariableOperation,
  "Subtract Variable": operations.SubVariableOperation,
  "Multiply Variable": operations.MultiplyVariableOperation,
  "Append Tables": operations.AppendTablesOperation,
}
POSSIBLE_OPERATIONS = OPERATIONS_MAPPING.keys()


class TaskFormat:
  
  def __init__(self, input_names: List[str], operation_name: str, operation_params: dict, output_name: str):
    self.input_names = input_names
    if not operation_name in POSSIBLE_OPERATIONS:
      raise Exception("Operation not available")
    self.operation_name = operation_name
    self.operation_params = operation_params
    self.output_name = output_name


class StageFormat:
  
  def __init__(self, name: str):
    self.name = name
    self.tasks:List[TaskFormat] = []

  def add_task_format(self, task_format: TaskFormat):
    self.tasks.append(task_format)


class PipelineFormat:
  
  def __init__(self):
    self.stages:List[StageFormat] = []

  def add_stage_format(self, stage_format: StageFormat):
    self.stages.append(stage_format)


class Task:
  
  def __init__(self, operation: operations.BaseOperation, task_format: TaskFormat):
    self.input_names = task_format.input_names
    self.operation = operation
    if not self.operation.validate_params(task_format.operation_params):
      raise Exception("Invalid params")
    self.operation_params = task_format.operation_params
    self.output_name = task_format.output_name

  def run(self, dd: data.DataDict) -> data.DataDict:
    if self.operation.get_type() == "SimpleOperation":
      input = dd.fetch_dt(self.input_names[0])
    elif self.operation.get_type() == "ComplexOperation":
      input = dd
    else:
      raise Exception("Invalid operation type")

    dt = self.operation.run(self.operation_params, input)
    dt.set_name(self.output_name)
    dd.edit_dt(dt)
    return dd


class Stage:

  def __init__(self, stage_format: StageFormat):
    self.name = stage_format.name
    self.task_list:List[Task] = []
    self.construct(stage_format)

  def construct(self, stage_format: StageFormat):
    for task_format in stage_format.tasks:
      task = Task(OPERATIONS_MAPPING[task_format.operation_name], task_format)
      self.task_list.append(task)

  def execute(self, data_dict: data.DataDict) -> data.DataDict:
    for task in self.task_list:
      data_dict = task.run(data_dict)
    return data_dict


class Pipeline:

  def __init__(self, name: str, input_data_dict: data.DataDict, pipeline_format: PipelineFormat):
    self.name = name
    self.data_dict = input_data_dict
    self.stage_list:List[Stage] = []
    self.construct(pipeline_format)

  def construct(self, pipeline_format: PipelineFormat):
    for stage_format in pipeline_format.stages:
      stage = Stage(stage_format)
      self.stage_list.append(stage)

  def execute(self) -> data.DataDict:
    for stage in self.stage_list:
      self.data_dict = stage.execute(self.data_dict)
    return self.data_dict