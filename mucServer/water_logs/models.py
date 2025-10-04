from django.db import models

# Create your models here.
class MucWaterLogs(models.Model):
    water_id = models.AutoField(primary_key=True)
    company_id = models.IntegerField()
    user_id = models.IntegerField()
    liters = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    water_cane = models.IntegerField(null=True, blank=True)
    created_on = models.BigIntegerField(default=0)
    modified_on = models.BigIntegerField(default=0)

    class Meta:
        managed = False   # ✅ Don't let Django create/alter the table
        db_table = 'muc_water_logs'  # ✅ Your actual table name
