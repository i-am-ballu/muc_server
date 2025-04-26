from rest_framework import serializers
from .models import MucUser, MucSuperAdmin

class MucUserSerializer(serializers.ModelSerializer):
    class Meta:
        model = MucUser
        fields = ['username', 'email', 'password']

class MucSuperAdminSerializer(serializers.ModelSerializer):
    class Meta:
        model = MucSuperAdmin
        fields = ['suparadmin_id', 'first_name', 'last_name', 'email', 'password', 'address', 'mobile_number', 'created', 'modified']

class ChangePasswordSerializer(serializers.Serializer):
    old_password = serializers.CharField(required=True)
    new_password = serializers.CharField(required=True)

class UserRoleSerializer(serializers.Serializer):
    email = serializers.EmailField()
    superadmin_id = serializers.IntegerField(required=False)
    admin_id = serializers.IntegerField(required=False)
    first_name = serializers.CharField()
    last_name = serializers.CharField()
    email = serializers.CharField()
    isSuperadmin = serializers.BooleanField()
    token = serializers.CharField()
