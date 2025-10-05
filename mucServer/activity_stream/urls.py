from django.urls import path
from . import views

urlpatterns = [
    path('getDistributionBasedOnUserId/', views.getDistributionBasedOnUserId),
    path('getSuperAdminSupportDetailsBasedOnCompany/', views.getSuperAdminSupportDetailsBasedOnCompany),
    path('getSuperAdminActivityStreamBasedOnCompany/', views.getSuperAdminActivityStreamBasedOnCompany),
    path('getAdminSupportDetailsBasedOnCompany/', views.getAdminSupportDetailsBasedOnCompany),
    path('getAdminActivityStreamBasedOnCompany/', views.getAdminActivityStreamBasedOnCompany),
    path('getInsightsWaterPayment/', views.getInsightsWaterPayment),
    path('getYearMonthListBasedOnUserId/', views.getYearMonthListBasedOnUserId),
]
