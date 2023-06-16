# add export csv
from django.contrib import admin
from .models import Ctgov1, Contacts, KeyWords
from django.http import HttpResponse
from django.utils.html import format_html
import csv

class Ctgov1Admin(admin.ModelAdmin):
    fields = ('nct_id', 'drank_final', 'brief_title', 'overall_status', 'phase', 'enrollment', 'description', 'url', 'updated', 'ts',  'new', 'emailab', 'alloc', 'action', 'notes')
    list_display = ('nct_id', 'url_brief_title', 'drank_final', 'enrollment', 'updated', 'notes', 'alloc',)
    list_filter = (['updated', 'alloc'])
    #readonly_fields = ['username',] # makes edit page use FK
    search_fields = ('nct_id','brief_title', 'official_title', 'description',)
    ordering = ('-drank_final',)
    search_help_text = 'Search by Trial ID, or text in Brief/Official title, Description.............?'

    @admin.display(description='username')
    def get_username(self, obj):
        return f"{obj.alloc.username}"

class CountryListFilter(admin.SimpleListFilter):
    title = 'Country (domain suffix)'
    parameter_name =  'ending in .'

    def lookups(self, request, model_admin):
        lus =(
            ('FR', 'France'),
            ('GB', 'UK'),
            ('IT', 'Italy'),
            ('CH', 'Swiss'),
            ('DE', 'Germany'),
            ('Au', 'Aussie'),
        )
        return lus

    def queryset(self, request, queryset):
        if self.value() == 'FR':
            return queryset.filter(email__endswith='fr').distinct()
        if self.value() == 'GB':
            return queryset.filter(email__endswith='uk')
        if self.value() == 'IT':
            return queryset.filter(email__endswith='it')
        if self.value() == 'CH':
            return queryset.filter(email__endswith='ch')
        if self.value() == 'DE':
            return queryset.filter(email__endswith='de')
        if self.value() == 'Au':
            return queryset.filter(email__endswith='au')

class DrankListFilter(admin.SimpleListFilter):
    title = 'Ranking (associated trial)'
    parameter_name =  '< rank >'

    def lookups(self, request, model_admin):
        lus =(
            ('<100', 'less than 100'),
            ('>100 <200', '100 to 200'),
            ('>200 <300', '200 to 300'),
            ('>300 <400', '300 to 400'),
            ('>400', 'over 400'),
        )
        return lus

    def queryset(self, request, queryset):
        if self.value() == '<100':
            return queryset.filter(nct_id__drank_final__lte=100)
        if self.value() == '>100 <200':
            return queryset.filter(nct_id__drank_final__lte=200).filter(nct_id__drank_final__gt=100)
        if self.value() == '>200 <300':
            return queryset.filter(nct_id__drank_final__lte=300).filter(nct_id__drank_final__gt=200)
        if self.value() == '>300 <400':
            return queryset.filter(nct_id__drank_final__lte=400).filter(nct_id__drank_final__gt=300)
        if self.value() == '>400':
            return queryset.filter(nct_id__drank_final__gt=400)

@admin.action(description='Export to CSV')
def export_csv(modeladmin, request, queryset):
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="contacts.csv"'
    writer = csv.writer(response)
    writer.writerow(['nct_id', 'contact_type', 'name', 'phone', 'email', 'rank', 'title'])
    cs = queryset.values_list('nct_id', 'contact_type', 'name', 'phone', 'email','nct_id__drank_final', 'nct_id__brief_title')
    for c in cs:
        writer.writerow(c)
    return response


class ContactsAdmin(admin.ModelAdmin):
    fields = ('nct_id', 'name', 'phone', 'email',)
    list_display = ('url_nct_id', 'name', 'phone', 'email','drank','get_brief_title',) # 'nct_id__drank_final')
    list_filter = ([CountryListFilter, DrankListFilter])
    search_fields = ('name', 'email', 'nct_id__nct_id',)
    #ordering = ('-ncd_id',)
    search_help_text = 'Search by Trial NCT_ID, Contacts, email.............?'
    actions = [export_csv,]

    @admin.display(description='Trial ID')
    def url_nct_id(self, obj):
        return format_html("<a href={} target=_blank>{}</a>", obj.nct_id.url, obj.nct_id.nct_id)

    @admin.display(description='Ranking')
    def drank(self, obj):
        return obj.nct_id.drank_final

    @admin.display(description='Brief Title')
    def get_brief_title(self, obj):
        return obj.nct_id.brief_title


class KeyWordsAdmin(admin.ModelAdmin):
    fields = ('kw', 'source')
    list_display = ('kw', 'source',)

admin.site.register(Ctgov1, Ctgov1Admin)
admin.site.register(Contacts, ContactsAdmin)
admin.site.register(KeyWords, KeyWordsAdmin)

#admin.site.site_header = "M+ Hub API : Device, Trials admin panels"

'''nct_id, drank_final, brief_title, overall_status, phase, enrollment, description, url, updated, ts,  new, emailab, action, notes,alloc
class Contacts(models.Model):
    id = models.AutoField(primary_key=True, max_length=12)
    nct_id = models.CharField(max_length=12, blank=True, null=True)
    contact_type = models.CharField(max_length=12)
    name = models.CharField(max_length=36, null=True)
    phone = models.CharField(max_length=36, null=True)
    email = models.CharField(max_length=36, null=True)


'''
