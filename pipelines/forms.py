from attr import field
from django import forms

from pipelines.models import Pipeline, Operation

# class PipelineForm(forms.Form):
#   name = forms.CharField()

#   def clean(self):
#     data = self.cleaned_data
#     name = data.get("name")

#     # Has to limited to a particular user and not global
#     qs = Pipeline.objects.filter(name__icontains=name)
#     if qs.exists():
#       self.add_error("name", f"{name} is already taken. Please pick another name.")
#     return data

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
        "placeholder": f'Pipeline {str(field)}',
        "class": 'form-control',
      }
      self.fields[str(field)].widget.attrs.update(
        new_data
      )


class OperationForm(forms.ModelForm):
  class Meta:
    model = Operation
    fields = ["stage_name", "operation_name", "column_name", "data_input_name", "data_output_name", "parameters"]
