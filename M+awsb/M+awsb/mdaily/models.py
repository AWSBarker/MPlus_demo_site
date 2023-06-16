# TODO fleet size
import pandas as pd
from django.db import models
from datetime import datetime as dt, timedelta
from django.db.models import Count, Min, F

class MDailyQuerySet(models.QuerySet):
    now = dt.now()

    def measured_today(self):
        yd =(self.now - timedelta(days=1)).date()
        return self.filter(last_measure_at__gte=yd).exclude(last_measure_at__gt = dt.now()) #.order_by('last_measure_at') # make_aware(dt(2021, 12, 2)))

    def measured_this_week(self):
        lw =(self.now - timedelta(days=7)).date()
        return self.filter(last_measure_at__gte=lw).exclude(last_measure_at__gt = dt.now()) #.order_by('last_measure_at')  # make_aware(dt(2021, 12, 2)))

    def measured_90d(self):
        d90 =(self.now - timedelta(days=90)).date()
        o = MDaily.objects.values('last_measure_at', 'count').filter(last_measure_at__gte=d90).exclude(last_measure_at__gt = dt.now())
        return pd.DataFrame(list(o)) #.order_by('last_measure_at')  # make_aware(dt(2021, 12, 2)))

    def measured_1d(self): # for 24hr heatmap plot
        d1 = (self.now - timedelta(days=1)).date()
        o = MDaily.objects.values('last_measure_at', 'count').filter(last_measure_at__gte=d1).exclude(
            last_measure_at__gt=dt.now())
        return pd.DataFrame(list(o))  # .order_by('last_measure_at')  # make_aware(dt(2021, 12, 2)))

    def measured_since(self): # used for byorg plot
        one_yr_ago =(self.now - timedelta(days=360)).date()
        df = pd.DataFrame(list(self.filter(last_measure_at__gte = one_yr_ago).exclude(last_measure_at__gt = self.now))) #.order_by('last_measure_at') # make_aware(dt(2021, 12, 2)))
        return df

    def measure1st_d(self): # first measure for Active devices by day
        fm = MDaily.objects.values('imei').exclude(last_measure_at=None).annotate(first=Min('last_measure_at'))
        df = pd.DataFrame(list(fm))
        df = df.groupby([pd.Grouper(key='first', freq='D')])['imei'].nunique().to_frame()
        df['xax'] = pd.to_datetime(df.index).date
        df['roll'] = df.imei.rolling(30).mean()
        return df

    def latest_measure(self): # returns a dict {last_measure_at : dt} due to future dates standard index (checked_on, latest_measure_at) not working
        return MDaily.objects.exclude(last_measure_at__gte = dt.now()).values('last_measure_at').latest('last_measure_at')

    def last_checked(self): # due to future dates standard index (checked_on, latest_measure_at) not working
        return MDaily.objects.values('checked_on').latest('checked_on')

    def wrong_date(self): # SELECT * FROM `M+_daily` where last_measure_at > checked_on AND last_measure_at > CURRENT_DATE
        return self.filter(last_measure_at__gt=F('checked_on')).exclude(last_measure_at__lte = dt.now()).exclude(checked_on__lte=self.now - timedelta(days=7)).order_by('-checked_on')

class MOrgQuerySet(models.QuerySet):
    def fleet_size(self):
        # TODO fleetsize based on M+deviceOwners to capture device movements during month
        #fs = MDaily.objects.all().values('org__showas').annotate(fs=Count('imei', distinct=True)).order_by('org')
        fs = MDevOwn.objects.all().values('owner__showas').annotate(fs=Count('imei', distinct=True))
        return fs

# class MDailyManager(models.Manager):
#     def get_queryset(self):
#        return super().get_queryset()
       #return self.filter(count__gt = 100)

class MDaily(models.Model):
    """
    - daily crontab fill Mapi2 calls
    - can be multi daily if imei count unique i.e. changed since last update overrides due to imei-count unique index
    """
    id = models.AutoField(primary_key=True, unique=True)
    imei = models.BigIntegerField()
    checked_on = models.DateTimeField()
    last_measure_at = models.DateTimeField(blank=True, null=True)
    count = models.SmallIntegerField()
    org = models.ForeignKey('MOrg', on_delete=models.DO_NOTHING, db_column='org', null=True, related_name='measures_of')

    objects = MDailyQuerySet.as_manager()
    #manager = AManager()
    def __str__(self):
         return f'{self.imei} {self.org}'

    class Meta:
        managed = True
        db_table = 'M+_daily'
        verbose_name_plural = "M+DailyMeasures"
        #label = 'Mdaily_options_label'
        #get_latest_by = ['checked_on', 'last_measure_at'] # not working due to future dates
        constraints = [models.UniqueConstraint(fields=['imei', 'count'], name="imei_count")]
        # unique_together = ('imei', 'count')

class MDevOwn(models.Model):
    """
    - only updates on API call Mapi2 Dev.update_owner !
    """

    imei = models.BigIntegerField(primary_key=True, unique=True)
    owner = models.ForeignKey('MOrg', on_delete=models.DO_NOTHING, db_column='owner')
    inv_id = models.ForeignKey('MInv', on_delete=models.CASCADE, null=True, db_column='inv_id')

    class Meta:
        managed = True
        db_table = 'M+_dev_own'
        verbose_name_plural = "M+DeviceOwners"

class MOrg(models.Model):

    id = models.SmallAutoField(primary_key=True, unique=True)
    org_id = models.CharField(max_length=64, default='', blank=False, db_column='org_id')
    name = models.CharField(max_length=12)
    showas = models.CharField(max_length=128, null=True)
    repid = models.IntegerField(db_column='repID')
    # Field name made lowercase. settings.AUTH_USER_MODEL, on_delete=models.CASCADE, db_column='repID'
    # name and showas need swapping length,
    objects = MOrgQuerySet.as_manager()

    def __str__(self):
        return self.name

    class Meta:
        managed = True
        db_table = 'M+_orgs'
        verbose_name_plural = "M+Orgs"

class MInv(models.Model):
    inv_id = models.SmallAutoField(primary_key=True, db_column='inv_id')
    sin = models.CharField(max_length=8)
    dated = models.DateField()
    sub_start = models.DateField(null=True)
    sub_duration = models.IntegerField(default=6)
    sub_end = models.DateField(null=True)
    sub_type = models.ForeignKey('MSub', default='ELIOT6', on_delete=models.CASCADE, db_column='sub_type')
    sub_active_rate = models.FloatField()
    sub_dorm_rate = models.FloatField()

    def __str__(self):
        return self.sin + ' dated: ' + str(self.dated)

    class Meta:
        managed = True
        db_table = 'M+_inv'
        verbose_name_plural = "M+Invoices"

class MSub(models.Model):
    name = models.CharField(primary_key=True, max_length=12)
    code = models.CharField(max_length=12)
    note = models.CharField(max_length=120)

    def __str__(self):
        return self.name + ' (' + self.note + ')'

    class Meta:
        managed = True
        db_table = 'M+_sub'
        verbose_name_plural = "M+Subscriptions"




'''    ELIOT6 ='E6'
    ELIOT12 = 'E12'
    UPFRONT = 'UP'
    PAYASUGO = 'UG'
    ELIOT24 = 'E24'
    SUB_CHOICES = [(ELIOT6, 'Eliot6'), (PAYASUGO, 'Pay as you go'),
                   (UPFRONT, '100% upfront'), (ELIOT12, 'ELIOT12'), (ELIOT24, 'ELIOT24'),]
'''
