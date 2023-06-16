from django import forms
from django.contrib.auth.forms import UserChangeForm, UserCreationForm
from .models import User
from django.db import models

class UserCreationForm(UserCreationForm):

    class Meta(UserCreationForm.Meta):
       model = User
       fields = UserCreationForm.Meta.fields + ('is_sales',)

class UserChangeForm(UserChangeForm):

    class Meta(UserChangeForm.Meta):
       model = User
       #fields = UserChangeForm.Meta.fields + ('is_sales',) - see add_fieldsets
