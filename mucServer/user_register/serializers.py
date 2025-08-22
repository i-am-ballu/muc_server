from rest_framework import serializers
from .models import MucUser
import time

class MucUserSerializer(serializers.ModelSerializer):
     # Add confirmPassword & agree so they can be received but not saved
    confirmPassword = serializers.CharField(write_only=True, required=False)
    agree = serializers.BooleanField(write_only=True, required=False)

    class Meta:
        model = MucUser
        fields = '__all__'
        read_only_fields = ('full_name', 'created_on', 'modified_on')

    def validate(self, data):
        """ Optional: check password confirmation """
        password = data.get("password")
        confirm = data.get("confirmPassword")
        if confirm and password != confirm:
            raise serializers.ValidationError("Passwords do not match.")
        return data

    def create(self, validated_data):
        # Remove extra fields
        validated_data.pop('confirmPassword', None)
        validated_data.pop('agree', None)

        # Set full_name
        first_name = validated_data.get('first_name', '') or ''
        last_name = validated_data.get('last_name', '') or ''
        validated_data['full_name'] = f"{first_name} {last_name}".strip()

        # Set timestamps
        now = int(time.time())
        validated_data['created_on'] = now
        validated_data['modified_on'] = now

        return super().create(validated_data)

    def update(self, instance, validated_data):
        # Remove extra fields
        validated_data.pop('confirmPassword', None)
        validated_data.pop('agree', None)

        # Update fields
        for attr, value in validated_data.items():
            setattr(instance, attr, value)

        # Always recalc full_name
        first_name = getattr(instance, 'first_name', '') or ''
        last_name = getattr(instance, 'last_name', '') or ''
        instance.full_name = f"{first_name} {last_name}".strip()

        # Update modified_on only
        instance.modified_on = int(time.time())

        instance.save()
        return instance
