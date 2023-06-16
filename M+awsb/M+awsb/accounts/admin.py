from django.contrib import admin
from .models import User
from django.contrib.auth.admin import UserAdmin
from .forms import UserChangeForm, UserCreationForm
from django.contrib.auth.models import Group

class UserAdmin(UserAdmin):
        model = User
        list_display = ('username', 'email', 'first_name', 'last_name', 'is_staff', 'is_sales')
        filter_horizontal = ('groups', 'user_permissions')
        #add_form = UserCreationForm
        #form = UserChangeForm

        fieldsets = UserAdmin.fieldsets + (
            (None, {'fields': ('is_sales',)}),
        )

        add_fieldsets = UserAdmin.add_fieldsets + (
            (None, {'fields': ('is_sales',)}),
        )
admin.site.register(User, UserAdmin)

"""    
        ((None, {'fields': get('username', 'password')}),
         ('Personal info', {'fields': ('first_name', 'last_name', 'email')}),
         ('Permissions', {'fields': ('is_active', 'is_staff', 'is_superuser','groups', 'user_permissions')}),
         ('Important dates', {'fields': ('last_login', 'date_joined')}),
         ('Additional info', {'fields': ('is_sales',)}),
# #        ('Group Permissions', {'classes': ('collapse',), 'fields': ('groups', 'user_permissions',)})
                      )
"""
        # add_fieldsets = ((None, {'fields': ('username', 'password1', 'password2')}),
        # ('Personal info', {'fields': ('first_name', 'last_name', 'email')}),
        # ('Permissions', {'fields': ('is_active', 'is_staff', 'is_superuser', 'groups', 'user_permissions')}),
        # ('Important dates', {'fields': ('last_login', 'date_joined')}),
        # ('Additional info', {'fields': ('is_sales',)}))

