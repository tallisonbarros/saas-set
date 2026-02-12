from django.urls import path

from . import views

urlpatterns = [
    path("", views.dashboard, name="app_rotas_dashboard"),
    path("conexao/", views.conexao, name="app_rotas_conexao"),
    path("ordenar/", views.ordenar_rotas, name="app_rotas_ordenar"),
    path("rota/<str:prefixo>/", views.rota_detalhe, name="app_rotas_detalhe"),
    path("mapeamentos/", views.mapeamentos, name="app_rotas_mapeamentos"),
]
