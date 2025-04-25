from django.shortcuts import render

# Create your views here.

from rest_framework import generics
from .models import MucUser, MucSuperAdmin
from .serializers import MucUserSerializer, MucSuperAdminSerializer, ChangePasswordSerializer
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.renderers import JSONRenderer
from django.db import connection
from django.conf import settings
import bcrypt

def print_last_query():
    print("Last SQL Query: ", connection.queries)

class MucUserRegisterView(generics.CreateAPIView):
    queryset = MucUser.objects.all()
    serializer_class = MucUserSerializer


class MucSuperAdminRegisterView(APIView):
    renderer_classes = [JSONRenderer]

    def get(self, request, pk=None):
        if pk:
            try:
                superadmin = MucSuperAdmin.objects.get(suparadmin_id=pk)
                data = {
                    'suparadmin_id': superadmin.suparadmin_id,
                    'first_name': superadmin.first_name,
                    'last_name': superadmin.last_name,
                    'email': superadmin.email,
                    'address': superadmin.address,
                    'mobile_number': superadmin.mobile_number,
                    'created': superadmin.created,
                    'modified': superadmin.modified,
                }
                return Response(data, status=status.HTTP_200_OK)
            except MucSuperAdmin.DoesNotExist:
                return Response({'error': 'Super Admin not found'}, status=status.HTTP_404_NOT_FOUND)

        # If no pk is provided, return all
        superadmins = MucSuperAdmin.objects.all()
        print_last_query();
        serializer = MucSuperAdminSerializer(superadmins, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def delete(self, request, pk):
        try:
            superadmin = MucSuperAdmin.objects.get(suparadmin_id=pk)
            superadmin.delete()
            return Response({'message': 'SuperAdmin deleted successfully'}, status=status.HTTP_204_NO_CONTENT)
        except Exception as e:
            return Response({'error': 'Super Admin not found'}, status=status.HTTP_404_NOT_FOUND)

    def post(self, request):
        email = request.data.get('email')
        password = request.data.get('password')
        try:
            superadmin = MucSuperAdmin.objects.get(email=email)
        except Exception as e:
            return Response({'error': 'Invalid email or password'}, status=status.HTTP_401_UNAUTHORIZED)
            # Check password using bcrypt

        if bcrypt.checkpw(password.encode('utf-8'), superadmin.password.encode('utf-8')):
            return Response({
                'message': 'Login successful',
                'superadmin_id': superadmin.suparadmin_id,
                'first_name': superadmin.first_name,
                'last_name': superadmin.last_name,
                'email': superadmin.email
            }, status=status.HTTP_200_OK)
        else:
            return Response({'error': 'Invalid email or password'}, status=status.HTTP_401_UNAUTHORIZED)

class SuperAdminChangePasswordView(APIView):
    def post(self, request):
        serializer = ChangePasswordSerializer(data=request.data)
        if serializer.is_valid():
            old_password = serializer.validated_data['old_password']
            new_password = serializer.validated_data['new_password']
            email = request.data.get('email')
            suparadmin_id = request.data.get('suparadmin_id')

            try:
                superadmin = MucSuperAdmin.objects.get(suparadmin_id=suparadmin_id,email=email)
            except MucSuperAdmin.DoesNotExist:
                return Response({'error': 'SuperAdmin not found'}, status=status.HTTP_404_NOT_FOUND)

            if bcrypt.checkpw(old_password.encode('utf-8'), superadmin.password.encode('utf-8')):
                hashed_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())
                superadmin.password = hashed_password.decode('utf-8')
                superadmin.save()
                return Response({'message': 'Password changed successfully'}, status=status.HTTP_200_OK)
            else:
                return Response({'error': 'Old password is incorrect'}, status=status.HTTP_400_BAD_REQUEST)

        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class MySQLHealthCheck(APIView):
    # Force the response to be in JSON format
    renderer_classes = [JSONRenderer]  # This line should be at the same indentation level as the class

    def get(self, request):
        try:
            # Ensure the database connection
            connection.ensure_connection()

            # Fetch database settings
            db_settings = settings.DATABASES['default']
            print('APIView ----- ', db_settings['HOST'], db_settings['PORT'], db_settings['NAME'], db_settings['USER'], db_settings['PASSWORD'])

            # Return a JSON response with the database details
            return Response({
                'status': 'connected',
                'host': db_settings['HOST'],
                'port': db_settings['PORT'],
                'database': db_settings['NAME'],
                'user': db_settings['USER'],
            })
        except Exception as e:
            # Return error message in JSON format
            return Response({'status': 'error', 'message': str(e)})
