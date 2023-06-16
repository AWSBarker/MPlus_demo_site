from django.contrib import admin
from .models import MOrg, MDaily, MDevOwn, MInv,MSub
from django.contrib.auth.models import Group

def edit_inv(modeladmin, request, queryset):
    queryset.update(sub_type = 'E6')

edit_inv.short_description = 'edit inv'

class mdevownAdmin(admin.ModelAdmin):
    list_display = ('imei', 'owner',)
    list_filter = (['owner'])
    search_fields = ['imei__endswith']
    ordering = ('-imei',)

class morgAdmin(admin.ModelAdmin):
    list_display = ('name', 'showas', 'org_id',  'repid', 'device_total',)

    @admin.display(description='Fleet size')
    def device_total(self, obj):
        # calc on M+_org
        res= MOrg.objects.fleet_size()
        res1 = list(res.filter(owner__name = obj.name).values_list('fs'))
        return res1[0] if res1 else 0

class minvAdmin(admin.ModelAdmin):
    list_display = ('sin', 'sub_start', 'sub_end', 'sub_type', 'sub_active_rate', 'sub_duration',)
    actions = [edit_inv]

class mdailyAdmin(admin.ModelAdmin):
    list_display = (['imei', 'checked_on', 'last_measure_at', 'count', 'org'])
    list_filter = (['org', 'checked_on', 'last_measure_at'])
    search_fields = ['imei__endswith']
    ordering = ('-checked_on',)

class msubAdmin(admin.ModelAdmin):
    list_display = (['name', 'code', 'note'])

admin.site.register(MOrg, morgAdmin)
admin.site.register(MDaily, mdailyAdmin)
admin.site.register(MDevOwn, mdevownAdmin)
admin.site.register(MInv, minvAdmin)
admin.site.register(MSub, msubAdmin)
#admin.site.site_header = "M+ admin panels"
