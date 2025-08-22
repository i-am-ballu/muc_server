from django.urls import path
from .views import (
    MucSuperAdminRegisterView,
    SuperAdminChangePasswordView,
    MySQLHealthCheck
)

urlpatterns = [
    path('register/superadmin/', MucSuperAdminRegisterView.as_view()),
    path('getSuperAdminDetails/', MucSuperAdminRegisterView.as_view(), name='get_all_superadmins'),
    path('getSuperAdminDetailsById/<int:pk>', MucSuperAdminRegisterView.as_view(), name='get_superadmin'),
    path('deleteSuperAdminDetailsById/<int:pk>', MucSuperAdminRegisterView.as_view(), name='delete_superadmin'),
    path('getsSuperAdminDetailsByEmailId', MucSuperAdminRegisterView.as_view(), name='get_superadmin_by_email_id'),
    path('upsertSuperAdminPassword', SuperAdminChangePasswordView.as_view(), name='change-superadmin-password'),
    path('check-db', MySQLHealthCheck.as_view())
]
