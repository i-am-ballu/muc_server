from django.urls import path
from . import views

urlpatterns = [
    path('payment-status/<int:company_id>/<int:user_id>/', views.user_payment_status),
]
