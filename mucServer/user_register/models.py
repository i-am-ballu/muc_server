from django.db import models

# Create your models here.
class MucUser(models.Model):
    user_id = models.AutoField(primary_key=True)
    company_id = models.IntegerField(default=0)
    first_name = models.CharField(max_length=50, blank=True, null=True)
    last_name = models.CharField(max_length=50, blank=True, null=True)
    full_name = models.CharField(max_length=50, blank=True, null=True)
    email = models.EmailField(max_length=150, unique=True)
    password = models.CharField(max_length=255)
    address = models.CharField(max_length=250, blank=True, null=True)
    mobile_number = models.CharField(max_length=15, blank=True, null=True)
    country = models.CharField(max_length=150)
    state = models.CharField(max_length=150)
    city = models.CharField(max_length=150)
    rate_per_cane = models.IntegerField(default=0)
    created_on = models.BigIntegerField(default=0)
    modified_on = models.BigIntegerField(default=0)
    class Meta:
        db_table = 'muc_user'  # Specify the table name in the database
        managed = False

    def __str__(self):
        return self.full_name or self.email
