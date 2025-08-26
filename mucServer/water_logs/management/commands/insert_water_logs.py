from django.core.management.base import BaseCommand
from django.utils.timezone import now
from user_register.models import MucUser
from water_logs.models import MucWaterLogs
from django.contrib.auth import get_user_model
import time
import datetime

User = get_user_model()

class Command(BaseCommand):
    help = "Insert daily water log records for all users"

    def handle(self, *args, **kwargs):
        now = int(time.time() * 1000)
        print(now)

        # Since __date lookup won’t work on BigInteger, compare ranges
        start_of_day = int(time.mktime(time.strptime(time.strftime("%Y-%m-%d 00:00:00"), "%Y-%m-%d %H:%M:%S"))) * 1000
        end_of_day = int(time.mktime(time.strptime(time.strftime("%Y-%m-%d 23:59:59"), "%Y-%m-%d %H:%M:%S"))) * 1000

        users = MucUser.objects.all()

        for user in users:
            # Check if already inserted today
            if not MucWaterLogs.objects.filter(
            user_id=user.user_id,
            created_on__gte=start_of_day,
            created_on__lte=end_of_day
            ).exists():
                MucWaterLogs.objects.create(
                    company_id=user.company_id,  # adjust field if required
                    user_id=user.user_id,
                    liters=0,
                    water_cane=0,
                    payment_status=0,  # 0 = pending
                    created_on=now,
                    modified_on=now,
                )
                self.stdout.write(self.style.SUCCESS(f"Inserted water log for user {user.user_id}"))
            else:
                self.stdout.write(self.style.WARNING(f"Water log already exists for user {user.user_id}"))
