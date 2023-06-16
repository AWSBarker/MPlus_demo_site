from django.db import models
from django.conf import settings
from django.utils.html import format_html
from django.contrib import admin

class Ctgov1(models.Model):
    nct_id = models.CharField(primary_key=True, unique=True, max_length=12, null=False, db_column='nct_id')
    id_out = models.IntegerField(blank=True, null=True)
    drank_out = models.IntegerField(db_column='Drank_out', blank=True, null=True)  # Field name made lowercase.
    id_desc = models.IntegerField(blank=True, null=True)
    drank_desc = models.IntegerField(db_column='Drank_desc', blank=True, null=True)  # Field name made lowercase.
    drank_final = models.IntegerField(db_column='Drank_final', blank=True, null=True, verbose_name='Ranking')  # Field name made lowercase.
    brief_title = models.TextField(blank=True, null=True)
    official_title = models.TextField(blank=True, null=True)
    overall_status = models.TextField(blank=True, max_length=48, null=True)
    phase = models.TextField(blank=True, max_length=14, null=True)
    enrollment = models.IntegerField()
    description = models.TextField(blank=True, null=True)
    url = models.URLField(max_length=68)
    updated = models.DateField(blank=True, null=True)
    ts = models.DateTimeField(blank=True, null=True)
    new = models.CharField(max_length=1)
    emailab = models.IntegerField(db_column='emailAB')  # Field name made lowercase.
    action = models.CharField(blank=True, max_length=14, null=True)
    notes = models.TextField(blank=True, null=True)
    alloc = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, db_column='Alloc', verbose_name='Allocated to')  # Field name made lowercase.

    def __str__(self):
        return self.nct_id + self.brief_title

    @admin.display(description='Brief Title (hyperlink)')
    def url_brief_title(self):
        return format_html("<a href={} target=_blank>{}</a>", self.url, self.brief_title)

    def get_absolute_url(self):
        from django.urls import reverse
        return reverse('trials:trial_detail', kwargs={'pk' : self.nct_id})

    class Meta:
        managed = True
        db_table = 'CTgov1'
        ordering = ['-drank_final']
        verbose_name_plural = 'Trials'
        verbose_name = "Trials"

class Contacts(models.Model):
    id = models.AutoField(primary_key=True)
    nct_id = models.ForeignKey(Ctgov1, on_delete=models.CASCADE, null=False, max_length=12, to_field='nct_id', db_column='nct_id', related_name='ids')
    contact_type = models.CharField(max_length=12)
    name = models.CharField(max_length=36, null=True)
    phone = models.CharField(max_length=36, null=True)
    email = models.CharField(max_length=36, null=True)

    def __str__(self):
          return self.name

    class Meta:
        managed = True
        db_table = 'CTgovContacts'
        ordering = ['-nct_id']
        verbose_name_plural = 'Contacts'
        verbose_name = "Contacts"
        constraints = [models.UniqueConstraint(fields=['nct_id', 'email'], name="nct_email")]

class KeyWords(models.Model):
    """
    for search filters in weekly run
    """
    CHOICES =(('desc', 'desc'), ('outcome', 'outcome'))

    id = models.AutoField(primary_key=True)
    kw = models.CharField(max_length=36)
    source = models.CharField(max_length=36, null=True, choices=CHOICES)

    def __str__(self):
          return self.kw

    class Meta:
        managed = True
        db_table = 'CTgovKW'
        verbose_name_plural = 'Keywords'
        verbose_name = "Keywords"



"""
Convert CTgov1 to work with accounts.Users

1. FK is CTgov1.Alloc which should be int (User.id)
2. copy Mtest3.accountscustomuser to bitnami.Health.account.customuser
3. Run sql below to convert Allow inputs


UPDATE CTgov1 SET Alloc = CASE WHEN 'Alloc' = 'AB' THEN 2
WHEN Alloc='LC' THEN 3
WHEN Alloc='GQ' THEN 4
WHEN Alloc='GL' THEN 5
WHEN Alloc='RK' THEN 6
WHEN Alloc='A2' THEN 7
WHEN Alloc='Ignore' THEN 8
WHEN Alloc='MS' THEN 9
WHEN Alloc='' THEN 99
END

"""

