from django.db import models

# Create your models here.

class MucSuperAdmin(models.Model):
    superadmin_id = models.AutoField(primary_key=True)
    first_name = models.CharField(max_length=50, null=True, blank=True)
    last_name = models.CharField(max_length=50, null=True, blank=True)
    email = models.CharField(max_length=150)
    password = models.CharField(max_length=255)
    address = models.CharField(max_length=250, null=True, blank=True)
    mobile_number = models.CharField(max_length=15, null=True, blank=True)
    created = models.BigIntegerField(default=0)
    modified = models.BigIntegerField(default=0)
    class Meta:
        db_table = 'superadmin'  # Specify the table name in the database
        managed = False

    def __str__(self):
        return f'{self.first_name} {self.last_name}'
