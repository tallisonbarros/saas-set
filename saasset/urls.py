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
from django.urls import include, path

from core import views

urlpatterns = [
    path('admin/explorar/', admin.site.admin_view(views.admin_explorar), name="admin_explorar"),
    path('admin/', admin.site.urls),
    path('', views.home, name="home"),
    path('api/ingest', views.api_ingest, name="api_ingest"),
    path('api/ingest/rules', views.api_ingest_rules, name="api_ingest_rules"),
    path('painel/', views.painel, name="painel"),
    path('ingest-gerenciar/', views.planta_conectada, name="ingest_gerenciar"),
    path('ingest-gerenciar/erros/', views.ingest_error_logs, name="ingest_erros"),
    path('ingest-gerenciar/<int:pk>/', views.ingest_detail, name="ingest_detail"),
    path('planta-conectada/', views.planta_conectada_redirect, name="planta_conectada"),
    path('apps/gerenciar/', views.apps_gerenciar, name="apps_gerenciar"),
    path('apps/appmilhaobla/', include('core.apps.app_milhao_bla.urls')),
    path('apps/<slug:slug>/', views.app_home, name="app_home"),
    path('login/', auth_views.LoginView.as_view(template_name="core/login.html"), name="login"),
    path('logout/', auth_views.LogoutView.as_view(), name="logout"),
    path('cadastre-se/', views.register, name="register"),
    path('propostas/', views.proposta_list, name="propostas"),
    path('propostas/nova/', views.proposta_nova_vendedor, name="proposta_nova_vendedor"),
    path('propostas/<int:pk>/', views.proposta_detail, name="proposta_detail"),
    path('propostas/<int:pk>/aprovar/', views.aprovar_proposta, name="aprovar_proposta"),
    path('propostas/<int:pk>/reprovar/', views.reprovar_proposta, name="reprovar_proposta"),
    path('propostas/<int:pk>/observacao/', views.salvar_observacao, name="salvar_observacao"),
    path('meu-perfil/', views.meu_perfil, name="meu_perfil"),
    path('ajustes/', views.ajustes_sistema, name="ajustes_sistema"),
    path('admin-logs/', views.admin_logs, name="admin_logs"),
    path('usuarios/', views.user_management, name="usuarios"),
    path('usuarios/<int:pk>/', views.usuarios_gerenciar_usuario, name="usuarios_gerenciar_usuario"),
    path('financeiro/', views.financeiro_overview, name="financeiro"),
    path('financeiro/nova/', views.financeiro_nova, name="financeiro_nova"),
    path('financeiro/cadernos/', views.financeiro_cadernos, name="financeiro_cadernos"),
    path('financeiro/cadernos/<int:pk>/', views.financeiro_caderno_detail, name="financeiro_caderno_detail"),
    path('financeiro/compras/<int:pk>/', views.financeiro_compra_detail, name="financeiro_compra_detail"),
    path('ios/', views.ios_list, name="ios_list"),
    path('ios/racks/<int:pk>/', views.ios_rack_detail, name="ios_rack_detail"),
    path('ios/racks/<int:pk>/lista/', views.ios_rack_io_list, name="ios_rack_io_list"),
    path('ios/modulos/', views.ios_modulos, name="ios_modulos"),
    path('ios/modulos/<int:pk>/', views.ios_modulo_modelo_detail, name="ios_modulo_modelo_detail"),
    path('ios/racks/modulos/<int:pk>/', views.ios_rack_modulo_detail, name="ios_rack_modulo_detail"),
    path('listas-ip/', views.listas_ip_list, name="listas_ip_list"),
    path('listas-ip/<int:pk>/', views.lista_ip_detail, name="lista_ip_detail"),
    path('radar-atividades/', views.radar_list, name="radar_list"),
    path('radar-atividades/<int:pk>/', views.radar_detail, name="radar_detail"),
    path(
        'radar-atividades/<int:radar_pk>/trabalhos/<int:pk>/',
        views.radar_trabalho_detail,
        name="radar_trabalho_detail",
    ),
    path('inventarios/', views.inventarios_list, name="inventarios_list"),
    path('inventarios/<int:pk>/', views.inventario_detail, name="inventario_detail"),
    path('inventarios/<int:pk>/tagset-preview/', views.inventario_tagset_preview, name="inventario_tagset_preview"),
    path('inventarios/<int:inventario_pk>/ativos/<int:pk>/', views.inventario_ativo_detail, name="inventario_ativo_detail"),
    path(
        'inventarios/<int:inventario_pk>/ativos/<int:ativo_pk>/itens/<int:pk>/',
        views.inventario_item_detail,
        name="inventario_item_detail",
    ),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
