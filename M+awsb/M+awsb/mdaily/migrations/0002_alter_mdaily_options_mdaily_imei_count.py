# Generated by Django 4.0.4 on 2022-06-21 18:25

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('mdaily', '0001_initial'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='mdaily',
            options={'get_latest_by': ['checked_on', 'last_measure_at'], 'managed': True, 'verbose_name_plural': 'M+DailyMeasures'},
        ),
        migrations.AddConstraint(
            model_name='mdaily',
            constraint=models.UniqueConstraint(fields=('imei', 'count'), name='imei_count'),
        ),
    ]
