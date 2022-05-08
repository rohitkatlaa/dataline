from ast import arg
from datetime import datetime
from django import forms
from pipelines.choices import OPERATION_CHOICES

from pipelines.models import Pipeline, Operation

def schema_params(required_params: dict, operation_params: dict) -> bool:
  '''
  required_params = {params : type}
  operation_params = {params : value}
  '''
  if len(required_params) != len(operation_params):
    return False
  for k, v in operation_params.items():
    if required_params.get(k) == None:
      return False
    elif type(v) != required_params[k]:
      return False
  return True

class PipelineForm(forms.ModelForm):
  error_css_class = 'error-field'
  required_css_class = 'required-field'

  # The attrs can be used to add classes to the form
  name = forms.CharField(widget=forms.TextInput())
  pipeline_description = forms.CharField(widget=forms.Textarea(attrs={"rows": 3}))
  class Meta:
    model = Pipeline
    fields = ["name", "pipeline_description"]

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    for field in self.fields:
      new_data = {
        "class": 'form-control',
      }
      self.fields[str(field)].widget.attrs.update(
        new_data
      )


class OperationForm(forms.ModelForm):

  operation_name = forms.ChoiceField(choices=OPERATION_CHOICES, initial='', required=True)
  class Meta:
    model = Operation
    fields = ["stage_name", "operation_name", "data_input_name", "data_output_name", "parameters"]

  def __init__(self, *args, **kwargs):
    super(OperationForm, self).__init__(*args, **kwargs)
    self.fields['stage_name'].widget.attrs.update({'class': 'form-control', 'style': "width:500px;"})
    self.fields['operation_name'].widget.attrs.update({'class': 'form-select', 'style': "width:500px;"})
    self.fields['data_input_name'].widget.attrs.update({'class': 'form-control', 'style': "width:500px;"})
    self.fields['data_output_name'].widget.attrs.update({'class': 'form-control', 'style': "width:500px;"})
    self.fields['parameters'].widget.attrs.update({'class': 'form-control', 'style': "width:500px;"})

  def clean(self):
    data = self.cleaned_data
    operation_name = data.get("operation_name")
    parameters = data.get("parameters")
    if operation_name == "Lower Case":
      required_params = {"column_name":str}
      if schema_params(required_params, parameters):
        pass
      else:
        self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")
    elif operation_name == "Upper Case":
      required_params = {"column_name":str}
      if schema_params(required_params, parameters):
        pass
      else:
        self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")
    
    elif operation_name == "Add Value":
      required_params = {"value":int, "column_name":str}
      if schema_params(required_params, parameters):
        pass
      else:
        self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")
    elif operation_name == "Multiply Value":
      required_params = {"value":int, "column_name":str, "precision":int}
      if schema_params(required_params, parameters):
        pass
      else:
        self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")
    elif operation_name == "Divide Value":
      required_params = {"value":int, "column_name":str, "precision":int}
      if schema_params(required_params, parameters):
        pass
      else:
        self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")
    
    elif operation_name == "Add Columns":
      if parameters.get("inplace") == True:
        required_params = {"value":int, "column_name1":str, "column_name2":str, "inplace":bool, "output_column":str}
        if schema_params(required_params, parameters):
          pass
        else:
          self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")
      elif parameters.get("inplace") == False:
        required_params = {"value":int, "column_name1":str, "column_name2":str, "inplace":bool}
        if schema_params(required_params, parameters):
          pass
        else:
          self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")
      else:
        self.add_error("parameters", f"invalid parameters are mentioned.")

    elif operation_name == "Multiply Columns":
      if parameters.get("inplace") == True:
        required_params = {"value":int, "column_name1":str, "column_name2":str, "inplace":bool, "output_column":str, "precision":int}
        if schema_params(required_params, parameters):
          pass
        else:
          self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")
      elif parameters.get("inplace") == False:
        required_params = {"value":int, "column_name1":str, "column_name2":str, "inplace":bool, "precision":int}
        if schema_params(required_params, parameters):
          pass
        else:
          self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")
      else:
        self.add_error("parameters", f"invalid parameters are mentioned.")

    elif operation_name == "Divide Columns":
      if parameters.get("inplace") == True:
        required_params = {"value":int, "column_name1":str, "column_name2":str, "inplace":bool, "output_column":str, "precision":int}
        if schema_params(required_params, parameters):
          pass
        else:
          self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")
      elif parameters.get("inplace") == False:
        required_params = {"value":int, "column_name1":str, "column_name2":str, "inplace":bool, "precision":int}
        if schema_params(required_params, parameters):
          pass
        else:
          self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")
      else:
        self.add_error("parameters", f"invalid parameters are mentioned.")

    elif operation_name == "Length of Text":
      required_params = {"column_name":str, "output_column":str}
      if schema_params(required_params, parameters):
        pass
      else:
        self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")

    elif operation_name == "Date Difference":
      required_params = {"column_name1":str, "column_name2":str, "output_column":str}
      if schema_params(required_params, parameters):
        pass
      else:
        self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")

    elif operation_name == "Add Date":
      required_params = {"column_name":str, "date":datetime}
      if schema_params(required_params, parameters):
        pass
      else:
        self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")

    elif operation_name == "Split Column by delimiter":
      required_params = {"column_name":str, "split_by":str}
      if schema_params(required_params, parameters):
        pass
      else:
        self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")

    elif operation_name == "Rename Column":
      required_params = {"column_name": str, "new_column_name": str}
      if schema_params(required_params, parameters):
        pass
      else:
        self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")

    elif operation_name == "Delete Column":
      required_params = {"column_name": str}
      if schema_params(required_params, parameters):
        pass
      else:
        self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")

    elif operation_name == "Integer Filter":
      required_params = {"column_name": str}
      if schema_params(required_params, parameters):
        pass
      else:
        self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")

    elif operation_name == "RegEx filter":
      required_params = {"column_name": str, "regex": str}
      if schema_params(required_params, parameters):
        pass
      else:
        self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")

    elif operation_name == "Join Tables":
      required_params = {"table1":str, "table2":str, "left_on":str, "right_on":str, "type":str}
      if schema_params(required_params, parameters):
        pass
      else:
        self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")

    elif operation_name == "Add Variable":
      required_params = {"table_name": str, "column_name": str, "variable": str}
      if schema_params(required_params, parameters):
        pass
      else:
        self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")

    elif operation_name == "Subtract Variable":
      required_params = {"table_name": str, "column_name": str, "variable": str}
      if schema_params(required_params, parameters):
        pass
      else:
        self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")

    elif operation_name == "Multiply Variable":
      required_params = {"table_name": str, "column_name": str, "variable": str}
      if schema_params(required_params, parameters):
        pass
      else:
        self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")

    elif operation_name == "Append Tables":
      required_params = {"input_tables":list, "output_name":str}
      if schema_params(required_params, parameters):
        pass
      else:
        self.add_error("parameters", f"{required_params} are the parameters that need to be mentioned.")

    return data


