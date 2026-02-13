from django.contrib import admin
from django.http import JsonResponse
from django.urls import path
from django.utils.translation import gettext_lazy as _

from .forms import PerfilUsuarioAdminForm
from .models import (
    Ativo,
    AtivoItem,
    CategoriaCompra,
    Caderno,
    CentroCusto,
    AdminAccessLog,
    PerfilUsuario,
    Inventario,
    InventarioID,
    ListaIP,
    ListaIPID,
    ListaIPItem,
    Radar,
    RadarAtividade,
    RadarClassificacao,
    RadarContrato,
    RadarID,
    RadarTrabalho,
    PlantaIO,
    FinanceiroID,
    Compra,
    Proposta,
    StatusCompra,
    TipoCompra,
    TipoPerfil,
    TipoAtivo,
    App,
)

admin.site.site_header = "SET Admin"
admin.site.site_title = "SET Admin"
admin.site.index_title = "Painel administrativo"


def _admin_get_perfil(user):
    if not user or not user.is_authenticated:
        return None
    try:
        return user.perfilusuario
    except PerfilUsuario.DoesNotExist:
        return None


class RadarOwnershipAdminMixin:
    owner_lookup = "cliente_id"

    def _owner_id(self, request):
        perfil = _admin_get_perfil(request.user)
        return perfil.id if perfil else None

    def _owns_obj(self, request, obj):
        if request.user.is_superuser:
            return True
        owner_id = self._owner_id(request)
        if not owner_id or obj is None:
            return False
        return getattr(obj, self.owner_lookup, None) == owner_id

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        if request.user.is_superuser:
            return qs
        owner_id = self._owner_id(request)
        if not owner_id:
            return qs.none()
        return qs.filter(**{self.owner_lookup: owner_id})

    def has_view_permission(self, request, obj=None):
        if request.user.is_superuser:
            return True
        if obj is None:
            return bool(self._owner_id(request))
        return self._owns_obj(request, obj)

    def has_change_permission(self, request, obj=None):
        if request.user.is_superuser:
            return True
        if obj is None:
            return bool(self._owner_id(request))
        return self._owns_obj(request, obj)

    def has_delete_permission(self, request, obj=None):
        return self.has_change_permission(request, obj=obj)

    def has_add_permission(self, request):
        if request.user.is_superuser:
            return True
        return bool(self._owner_id(request))


@admin.register(PerfilUsuario)
class PerfilUsuarioAdmin(admin.ModelAdmin):
    form = PerfilUsuarioAdminForm
    list_display = ("nome", "email", "tipos_display", "ativo")
    search_fields = ("nome", "email")

    def tipos_display(self, obj):
        return ", ".join(obj.tipos.values_list("nome", flat=True))
    tipos_display.short_description = "Tipos"


@admin.register(Proposta)
class PropostaAdmin(admin.ModelAdmin):
    change_form_template = "admin/core/proposta/change_form.html"
    list_display = (
        "nome",
        "codigo",
        "cliente",
        "aprovacao_display",
        "finalizada",
        "prioridade",
        "valor",
        "criado_em",
        "decidido_em",
    )
    list_filter = ("aprovada", "finalizada")
    search_fields = ("nome", "codigo", "cliente__nome")

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                "gerar-codigo/",
                self.admin_site.admin_view(self.gerar_codigo),
                name="core_proposta_gerar_codigo",
            )
        ]
        return custom_urls + urls

    def gerar_codigo(self, request):
        cliente_id = request.GET.get("cliente_id")
        if not cliente_id:
            return JsonResponse({"error": _("Selecione um cliente.")}, status=400)
        try:
            cliente = PerfilUsuario.objects.get(pk=cliente_id)
        except PerfilUsuario.DoesNotExist:
            return JsonResponse({"error": _("Cliente invalido.")}, status=400)
        proposta = Proposta(cliente=cliente)
        return JsonResponse({"codigo": proposta._proximo_codigo()})

    def aprovacao_display(self, obj):
        if obj.aprovada is True:
            return "Aprovada"
        if obj.aprovada is False:
            return "Reprovada"
        return "Pendente"
    aprovacao_display.short_description = "Aprovacao"


@admin.register(TipoPerfil)
class TipoPerfilAdmin(admin.ModelAdmin):
    list_display = ("nome",)
    search_fields = ("nome",)


@admin.register(App)
class AppAdmin(admin.ModelAdmin):
    list_display = ("nome", "slug", "ativo", "icon", "logo", "theme_color", "criado_em")
    search_fields = ("nome", "slug")
    list_filter = ("ativo",)


@admin.register(PlantaIO)
class PlantaIOAdmin(admin.ModelAdmin):
    list_display = ("codigo",)
    search_fields = ("codigo",)


@admin.register(InventarioID)
class InventarioIDAdmin(admin.ModelAdmin):
    list_display = ("codigo",)
    search_fields = ("codigo",)


@admin.register(ListaIPID)
class ListaIPIDAdmin(admin.ModelAdmin):
    list_display = ("codigo",)
    search_fields = ("codigo",)


@admin.register(ListaIP)
class ListaIPAdmin(admin.ModelAdmin):
    list_display = ("nome", "cliente", "id_listaip", "faixa_inicio", "faixa_fim", "criado_em")
    search_fields = ("nome", "cliente__nome", "id_listaip__codigo")


@admin.register(ListaIPItem)
class ListaIPItemAdmin(admin.ModelAdmin):
    list_display = ("ip", "lista", "nome_equipamento", "descricao", "mac", "protocolo")
    search_fields = ("ip", "lista__nome", "nome_equipamento", "descricao", "mac", "protocolo")


@admin.register(RadarID)
class RadarIDAdmin(admin.ModelAdmin):
    list_display = ("codigo",)
    search_fields = ("codigo",)


@admin.register(RadarContrato)
class RadarContratoAdmin(admin.ModelAdmin):
    list_display = ("nome",)
    search_fields = ("nome",)


@admin.register(RadarClassificacao)
class RadarClassificacaoAdmin(admin.ModelAdmin):
    list_display = ("nome",)
    search_fields = ("nome",)


@admin.register(Radar)
class RadarAdmin(RadarOwnershipAdminMixin, admin.ModelAdmin):
    owner_lookup = "cliente_id"
    list_display = ("nome", "cliente", "id_radar", "local", "criado_em")
    search_fields = ("nome", "cliente__nome", "id_radar__codigo", "local")

    def formfield_for_foreignkey(self, db_field, request, **kwargs):
        if db_field.name == "cliente" and not request.user.is_superuser:
            owner_id = self._owner_id(request)
            kwargs["queryset"] = PerfilUsuario.objects.filter(pk=owner_id) if owner_id else PerfilUsuario.objects.none()
        return super().formfield_for_foreignkey(db_field, request, **kwargs)

    def save_model(self, request, obj, form, change):
        if not request.user.is_superuser:
            owner_id = self._owner_id(request)
            if owner_id:
                obj.cliente_id = owner_id
        super().save_model(request, obj, form, change)


@admin.register(RadarTrabalho)
class RadarTrabalhoAdmin(RadarOwnershipAdminMixin, admin.ModelAdmin):
    owner_lookup = "radar__cliente_id"
    list_display = (
        "nome",
        "radar",
        "classificacao",
        "contrato",
        "setor",
        "responsavel",
        "status",
        "data_registro",
        "criado_em",
    )
    list_filter = ("status",)
    search_fields = ("nome", "radar__nome")

    def formfield_for_foreignkey(self, db_field, request, **kwargs):
        if db_field.name == "radar" and not request.user.is_superuser:
            owner_id = self._owner_id(request)
            kwargs["queryset"] = Radar.objects.filter(cliente_id=owner_id) if owner_id else Radar.objects.none()
        return super().formfield_for_foreignkey(db_field, request, **kwargs)


@admin.register(RadarAtividade)
class RadarAtividadeAdmin(RadarOwnershipAdminMixin, admin.ModelAdmin):
    owner_lookup = "trabalho__radar__cliente_id"
    list_display = ("nome", "trabalho", "status", "inicio_execucao_em", "finalizada_em", "horas_trabalho", "criado_em")
    list_filter = ("status",)
    search_fields = ("nome", "trabalho__nome")

    def formfield_for_foreignkey(self, db_field, request, **kwargs):
        if db_field.name == "trabalho" and not request.user.is_superuser:
            owner_id = self._owner_id(request)
            kwargs["queryset"] = (
                RadarTrabalho.objects.filter(radar__cliente_id=owner_id) if owner_id else RadarTrabalho.objects.none()
            )
        return super().formfield_for_foreignkey(db_field, request, **kwargs)


@admin.register(Inventario)
class InventarioAdmin(admin.ModelAdmin):
    list_display = ("nome", "cliente", "id_inventario", "cidade", "estado", "pais", "criado_em")
    search_fields = ("nome", "cliente__nome", "id_inventario__codigo")


@admin.register(Ativo)
class AtivoAdmin(admin.ModelAdmin):
    list_display = ("nome", "inventario", "tipo", "setor", "comissionado", "em_manutencao")
    search_fields = ("nome", "inventario__nome", "identificacao", "tag_interna", "tag_set")


@admin.register(AtivoItem)
class AtivoItemAdmin(admin.ModelAdmin):
    list_display = ("nome", "ativo", "tipo", "comissionado", "em_manutencao")
    search_fields = ("nome", "ativo__nome", "identificacao", "tag_interna", "tag_set")


@admin.register(TipoAtivo)
class TipoAtivoAdmin(admin.ModelAdmin):
    list_display = ("nome", "codigo", "ativo")
    search_fields = ("nome", "codigo")
    list_filter = ("ativo",)


@admin.register(FinanceiroID)
class FinanceiroIDAdmin(admin.ModelAdmin):
    list_display = ("codigo",)
    search_fields = ("codigo",)


@admin.register(Caderno)
class CadernoAdmin(admin.ModelAdmin):
    list_display = ("nome", "criador", "id_financeiro", "ativo", "criado_em")
    search_fields = ("nome", "criador__nome", "id_financeiro__codigo")
    list_filter = ("ativo",)


@admin.register(CategoriaCompra)
class CategoriaCompraAdmin(admin.ModelAdmin):
    list_display = ("nome",)
    search_fields = ("nome",)


@admin.register(TipoCompra)
class TipoCompraAdmin(admin.ModelAdmin):
    list_display = ("nome",)
    search_fields = ("nome",)


@admin.register(CentroCusto)
class CentroCustoAdmin(admin.ModelAdmin):
    list_display = ("nome",)
    search_fields = ("nome",)


@admin.register(StatusCompra)
class StatusCompraAdmin(admin.ModelAdmin):
    list_display = ("nome", "ativo")
    search_fields = ("nome",)
    list_filter = ("ativo",)


@admin.register(Compra)
class CompraAdmin(admin.ModelAdmin):
    list_display = ("nome", "descricao", "caderno", "total_itens", "data", "status_label")
    list_filter = ("categoria",)
    search_fields = ("nome", "descricao", "caderno__nome")

    def status_label(self, obj):
        itens = list(obj.itens.all())
        if itens and all(item.pago for item in itens):
            return "Pago"
        return "Pendente"
    status_label.short_description = "Status"

    def total_itens(self, obj):
        total = 0
        for item in obj.itens.all():
            total += (item.valor or 0) * (item.quantidade or 0)
        return total
    total_itens.short_description = "Total"


@admin.register(AdminAccessLog)
class AdminAccessLogAdmin(admin.ModelAdmin):
    list_display = ("created_at", "user", "module")
    list_filter = ("module",)
    search_fields = ("user__username", "module")
    ordering = ("-created_at",)
