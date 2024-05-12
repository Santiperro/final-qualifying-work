from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from . import views


urlpatterns = [
    path('', views.find_patterns, name='find_patterns'), 
    path('request-data', views.request_data, name='request_data'),
    path('load-data-submit', views.load_data_submit, name='load_data_submit'),  # Добавьте эту строку
    path('delete-sample/<int:id>', views.delete_sample, name='delete_sample'),
    path('find-patterns-submit', views.find_patterns_submit, name='find_patterns_submit'),
    
    # path('accounts/', include("django.contrib.auth.urls")),   # working for login.html
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)