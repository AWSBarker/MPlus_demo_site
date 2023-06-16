from django.db import models
from django.contrib.auth.models import AbstractUser, PermissionsMixin
from .managers import CustomUserManager

class User(AbstractUser, PermissionsMixin):
    """
    For emails etc  Add ewxtra fields here, makeigration, migrate
    """
    is_sales = models.BooleanField(default=False, help_text='select to enable this user to receive sales related stuff i.e. CTGov trials emails')

#    objects = CustomUserManager()

    def __str__(self):
        return self.username

