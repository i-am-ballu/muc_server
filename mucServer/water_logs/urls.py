from django.urls import path
from . import views

urlpatterns = [
    path('payment-status/', views.get_user_payment_status),
    path('upsert-log/', views.upsert_water_log_details),
    path('insert_payments/', views.insert_payments)
]
