"""
URL configuration for saasset project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/6.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.contrib.auth import views as auth_views
from django.urls import path

from core import views

urlpatterns = [
    path('admin/explorar/', admin.site.admin_view(views.admin_explorar), name="admin_explorar"),
    path('admin/', admin.site.urls),
    path('', views.home, name="home"),
    path('painel/', views.painel, name="painel"),
    path('login/', auth_views.LoginView.as_view(template_name="core/login.html"), name="login"),
    path('logout/', auth_views.LogoutView.as_view(), name="logout"),
    path('propostas/', views.proposta_list, name="propostas"),
    path('propostas/<int:pk>/', views.proposta_detail, name="proposta_detail"),
    path('propostas/<int:pk>/aprovar/', views.aprovar_proposta, name="aprovar_proposta"),
    path('propostas/<int:pk>/reprovar/', views.reprovar_proposta, name="reprovar_proposta"),
    path('propostas/<int:pk>/observacao/', views.salvar_observacao, name="salvar_observacao"),
    path('usuarios/', views.user_management, name="usuarios"),
    path('financeiro/', views.financeiro_overview, name="financeiro"),
    path('financeiro/nova/', views.financeiro_nova, name="financeiro_nova"),
    path('financeiro/cadernos/', views.financeiro_cadernos, name="financeiro_cadernos"),
    path('financeiro/cadernos/<int:pk>/', views.financeiro_caderno_detail, name="financeiro_caderno_detail"),
    path('financeiro/compras/<int:pk>/', views.financeiro_compra_detail, name="financeiro_compra_detail"),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
