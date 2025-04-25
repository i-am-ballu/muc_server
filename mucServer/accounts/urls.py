from django.urls import path
from .views import (
    MucUserRegisterView,
    MucSuperAdminRegisterView,
    MySQLHealthCheck
)

urlpatterns = [
    path('register/user/', MucUserRegisterView.as_view()),
    path('register/superadmin/', MucSuperAdminRegisterView.as_view()),
    path('getSuperAdminDetails', MucSuperAdminRegisterView.as_view(), name='get_all_superadmins'),
    path('getSuperAdminDetailsById/<int:pk>/', MucSuperAdminRegisterView.as_view(), name='get_superadmin'),
    path('check-db', MySQLHealthCheck.as_view())
]