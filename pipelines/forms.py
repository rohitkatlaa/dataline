from django import forms

from pipelines.models import Pipeline

class PipelineForm(forms.Form):
  name = forms.CharField()

  def clean(self):
    data = self.cleaned_data
    name = data.get("name")

    # Has to limited to a particular user and not global
    qs = Pipeline.objects.filter(name__icontains=name)
    if qs.exists():
      self.add_error("name", f"{name} is already taken. Please pick another name.")
    return data
