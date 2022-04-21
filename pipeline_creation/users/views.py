from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.forms import AuthenticationForm, UserCreationForm
from django.shortcuts import render, redirect

def register_view(request):
  form = UserCreationForm(request.POST or None)
  if form.is_valid():
    user_obj = form.save()
    return redirect('/accounts/login')

  context = {
    "form": form
  }
  return render(request, "users/register.html", context)

def login_view(request):
  if request.method == "POST":
    form = AuthenticationForm(request, data=request.POST)
    if form.is_valid():
      user = form.get_user()
      login(request, user)
      return redirect('/')
  else:
    form = AuthenticationForm(request)
  context = {
    "form": form
  }
  return render(request, "users/login.html", context)


def logout_view(request):
  if request.method == "POST":
    logout(request)
    return redirect("/accounts/login/")
  return render(request, "users/logout.html", {})