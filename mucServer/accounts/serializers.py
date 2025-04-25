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