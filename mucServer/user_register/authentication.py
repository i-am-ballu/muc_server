from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework import exceptions
from accounts.models import MucSuperAdmin
from user_register.models import MucUser

class CustomJWTAuthentication(JWTAuthentication):
    """
    Custom JWT Authentication to support both MucSuperAdmin and MucUser models.
    """
    def get_user(self, validated_token):
        user_id = validated_token.get("user_id")
        is_superadmin = validated_token.get("isSuperadmin", 0)

        if is_superadmin:
            try:
                return MucSuperAdmin.objects.get(superadmin_id=user_id)
            except MucSuperAdmin.DoesNotExist:
                raise exceptions.AuthenticationFailed("SuperAdmin not found")
        else:
            try:
                return MucUser.objects.get(user_id=user_id)
            except MucUser.DoesNotExist:
                raise exceptions.AuthenticationFailed("User not found")
