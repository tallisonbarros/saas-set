import calendar
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.http import HttpResponseForbidden, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from urllib.parse import urlencode
from django.utils import timezone
from django.views.decorators.http import require_POST

from django.contrib.auth.models import User
from django.db.models import Case, Count, DecimalField, F, IntegerField, OuterRef, Q, Subquery, Sum, Value, When
from django.db.models.expressions import ExpressionWrapper

from .forms import RegisterForm, TipoPerfilCreateForm, UserCreateForm
from .models import (
    CanalRackIO,
    CategoriaCompra,
    Caderno,
    CentroCusto,
    PerfilUsuario,
    Compra,
    CompraItem,
    ModuloIO,
    ModuloRackIO,
    FinanceiroID,
    Inventario,
    InventarioID,
    PlantaIO,
    Proposta,
    PropostaAnexo,
    RackIO,
    RackSlotIO,
    TipoCompra,
    TipoCanalIO,
    TipoPerfil,
    Ativo,
)


def _get_cliente(user):
    try:
        return user.perfilusuario
    except PerfilUsuario.DoesNotExist:
        email = (user.email or user.username or "").strip().lower()
        if not email:
            return None
        return PerfilUsuario.objects.filter(email__iexact=email).first()


def _user_role(user):
    if user.is_superuser or user.is_staff:
        return "ADMIN"
    cliente = _get_cliente(user)
    if not cliente:
        return "CLIENTE"
    has_cliente = cliente.tipos.filter(nome__iexact="Contratante").exists() or cliente.tipos.filter(
        nome__iexact="Cliente"
    ).exists()
    has_financeiro = cliente.tipos.filter(nome__iexact="Financeiro").exists()
    has_vendedor = cliente.tipos.filter(nome__iexact="Vendedor").exists()
    if has_cliente:
        return "CLIENTE"
    if has_financeiro:
        return "FINANCEIRO"
    if has_vendedor:
        return "VENDEDOR"
    return "CLIENTE"


def _has_tipo(user, nome):
    cliente = _get_cliente(user)
    if not cliente:
        return False
    return cliente.tipos.filter(nome__iexact=nome).exists()


def _has_tipo_any(user, nomes):
    cliente = _get_cliente(user)
    if not cliente:
        return False
    return cliente.tipos.filter(nome__in=nomes).exists()


def _ensure_default_cadernos(cliente):
    if not cliente:
        return
    capex = Caderno.objects.filter(nome="CAPEX", criador=cliente).first()
    if not capex:
        Caderno.objects.create(nome="CAPEX", ativo=True, criador=cliente)
    opex = Caderno.objects.filter(nome="OPEX", criador=cliente).first()
    if not opex:
        Caderno.objects.create(nome="OPEX", ativo=True, criador=cliente)


def _compra_status_label(compra):
    itens = list(compra.itens.all())
    if itens and all(item.pago for item in itens):
        return "Pago"
    return "Pendente"


def _add_months(base_date, months):
    month_index = (base_date.month - 1) + months
    year = base_date.year + (month_index // 12)
    month = (month_index % 12) + 1
    last_day = calendar.monthrange(year, month)[1]
    day = min(base_date.day, last_day)
    return date(year, month, day)


def home(request):
    if request.user.is_authenticated:
        logout(request)
    return render(request, "core/home.html")


@login_required
def painel(request):
    cliente = _get_cliente(request.user)
    display_name = None
    if cliente and cliente.nome:
        display_name = cliente.nome
    else:
        display_name = request.user.first_name or request.user.username
    return render(
        request,
        "core/painel.html",
        {
            "display_name": display_name,
            "role": _user_role(request.user),
            "is_financeiro": True,
            "is_cliente": True,
            "is_vendedor": True,
        },
    )


def register(request):
    if request.user.is_authenticated:
        return redirect("painel")
    message = None
    form = RegisterForm()
    if request.method == "POST":
        form = RegisterForm(request.POST)
        if form.is_valid():
            user = form.save()
            nome = form.cleaned_data["nome"].strip()
            empresa = form.cleaned_data.get("empresa", "").strip()
            perfil = PerfilUsuario.objects.create(
                nome=nome,
                email=user.email,
                usuario=user,
                ativo=True,
                empresa=empresa,
            )
            _ensure_default_cadernos(perfil)
            authenticated = authenticate(request, username=user.username, password=form.cleaned_data["senha"])
            if authenticated:
                login(request, authenticated)
                return redirect("painel")
            return redirect("login")
        message = "Revise os campos e tente novamente."
    return render(
        request,
        "core/register.html",
        {
            "form": form,
            "message": message,
        },
    )


@login_required
def ios_list(request):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")

    message = None
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_rack":
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            id_planta_raw = request.POST.get("id_planta", "").strip()
            slots_raw = request.POST.get("slots_total", "").strip()
            try:
                slots_total = int(slots_raw)
            except (TypeError, ValueError):
                slots_total = None
            if slots_total is not None:
                slots_total = max(1, min(60, slots_total))
            if nome and slots_total:
                planta = None
                if id_planta_raw:
                    planta, _ = PlantaIO.objects.get_or_create(codigo=id_planta_raw.upper())
                rack = RackIO.objects.create(
                    cliente=cliente,
                    nome=nome,
                    descricao=descricao,
                    id_planta=planta,
                    slots_total=slots_total,
                )
                slots = [RackSlotIO(rack=rack, posicao=index) for index in range(1, slots_total + 1)]
                RackSlotIO.objects.bulk_create(slots)
            return redirect("ios_list")
        if action == "create_channel_type":
            nome = request.POST.get("nome", "").strip().upper()
            if nome:
                TipoCanalIO.objects.get_or_create(nome=nome, defaults={"ativo": True})
            return redirect("ios_list")

    if request.user.is_staff and not cliente:
        racks = RackIO.objects.all()
    else:
        racks = RackIO.objects.filter(Q(cliente=cliente) | Q(id_planta__in=cliente.plantas.all()))
    racks = racks.annotate(ocupados=Count("slots", filter=Q(slots__modulo__isnull=False)))
    channel_types = TipoCanalIO.objects.filter(ativo=True).order_by("nome")
    search_term = request.GET.get("q", "").strip()
    search_results = []
    search_count = 0
    if search_term:
        slot_pos_subquery = RackSlotIO.objects.filter(modulo_id=OuterRef("modulo_id")).values("posicao")[:1]
        search_filter = (
            Q(nome__icontains=search_term)
            | Q(modulo__nome__icontains=search_term)
            | Q(modulo__modulo_modelo__nome__icontains=search_term)
            | Q(modulo__rack__nome__icontains=search_term)
        )
        channels = (
            CanalRackIO.objects.filter(modulo__rack__in=racks)
            .filter(search_filter)
            .select_related("modulo", "modulo__rack", "modulo__modulo_modelo", "tipo")
            .annotate(slot_pos=Subquery(slot_pos_subquery))
            .order_by("modulo__rack__nome", "slot_pos", "indice")[:200]
        )
        search_results = list(channels)
        search_count = len(search_results)
    return render(
        request,
        "core/ios_list.html",
        {
            "racks": racks,
            "channel_types": channel_types,
            "can_manage": bool(cliente),
            "search_term": search_term,
            "search_results": search_results,
            "search_count": search_count,
        },
    )


@login_required
def ios_rack_detail(request, pk):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    if cliente:
        rack = get_object_or_404(
            RackIO,
            Q(pk=pk),
            Q(cliente=cliente) | Q(id_planta__in=cliente.plantas.all()),
        )
    else:
        rack = get_object_or_404(RackIO, pk=pk)
    message = None
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "update_rack":
            if not request.user.is_staff and rack.cliente != cliente:
                return HttpResponseForbidden("Sem permissao.")
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            id_planta_raw = request.POST.get("id_planta", "").strip()
            slots_raw = request.POST.get("slots_total", "").strip()
            try:
                slots_total = int(slots_raw)
            except (TypeError, ValueError):
                slots_total = None
            if slots_total is not None:
                slots_total = max(1, min(60, slots_total))
            if nome:
                rack.nome = nome
            rack.descricao = descricao
            if id_planta_raw:
                planta, _ = PlantaIO.objects.get_or_create(codigo=id_planta_raw.upper())
                rack.id_planta = planta
            else:
                rack.id_planta = None
            if slots_total is not None and slots_total != rack.slots_total:
                if slots_total > rack.slots_total:
                    novos = [
                        RackSlotIO(rack=rack, posicao=index)
                        for index in range(rack.slots_total + 1, slots_total + 1)
                    ]
                    RackSlotIO.objects.bulk_create(novos)
                else:
                    slots_para_remover = rack.slots.filter(posicao__gt=slots_total).order_by("posicao")
                    if slots_para_remover.filter(modulo__isnull=False).exists():
                        message = "Nao foi possivel reduzir: existem slots ocupados acima do novo limite."
                        slots = rack.slots.select_related("modulo", "modulo__modulo_modelo").order_by("posicao")
                        modules = (
                            ModuloIO.objects.filter(Q(cliente=rack.cliente) | Q(is_default=True))
                            .select_related("tipo_base")
                            .order_by("nome")
                        )
                        ocupados = rack.slots.filter(modulo__isnull=False).count()
                        slots_livres = max(rack.slots_total - ocupados, 0)
                        return render(
                            request,
                            "core/ios_rack_detail.html",
                            {
                                "rack": rack,
                                "slots": slots,
                                "modules": modules,
                                "ocupados": ocupados,
                                "slots_livres": slots_livres,
                                "message": message,
                            },
                        )
                    slots_para_remover.delete()
                rack.slots_total = slots_total
            rack.save(update_fields=["nome", "descricao", "id_planta", "slots_total"])
            return redirect("ios_rack_detail", pk=rack.pk)
        if action == "delete_rack":
            if not request.user.is_staff and rack.cliente != cliente:
                return HttpResponseForbidden("Sem permissao.")
            rack.delete()
            return redirect("ios_list")
        if action in ["add_first", "add_to_slot"]:
            module_id = request.POST.get("module_id")
            module_modelo = get_object_or_404(
                ModuloIO.objects.filter(Q(cliente=rack.cliente) | Q(is_default=True)),
                pk=module_id,
            )
            slot = None
            if action == "add_to_slot":
                slot_id = request.POST.get("slot_id")
                slot = get_object_or_404(RackSlotIO, pk=slot_id, rack=rack)
                if slot.modulo_id:
                    return redirect("ios_rack_detail", pk=rack.pk)
            else:
                slot = RackSlotIO.objects.filter(rack=rack, modulo__isnull=True).order_by("posicao").first()
            if slot:
                modulo = ModuloRackIO.objects.create(
                    rack=rack,
                    modulo_modelo=module_modelo,
                    nome=module_modelo.nome,
                )
                canais = [
                    CanalRackIO(
                        modulo=modulo,
                        indice=index,
                        nome=f"Canal {index:02d}",
                        tipo=module_modelo.tipo_base,
                    )
                    for index in range(1, module_modelo.quantidade_canais + 1)
                ]
                CanalRackIO.objects.bulk_create(canais)
                slot.modulo = modulo
                slot.save(update_fields=["modulo"])
            return redirect("ios_rack_detail", pk=rack.pk)
        if action == "assign_modules":
            if not request.user.is_staff and rack.cliente != cliente:
                return HttpResponseForbidden("Sem permissao.")
            modules_qs = ModuloIO.objects.filter(Q(cliente=rack.cliente) | Q(is_default=True))
            for key, value in request.POST.items():
                if not key.startswith("slot_"):
                    continue
                if not value:
                    continue
                try:
                    slot_id = int(key.split("_", 1)[1])
                except (TypeError, ValueError):
                    continue
                slot = RackSlotIO.objects.filter(pk=slot_id, rack=rack, modulo__isnull=True).first()
                if not slot:
                    continue
                module_modelo = modules_qs.filter(pk=value).first()
                if not module_modelo:
                    continue
                modulo = ModuloRackIO.objects.create(
                    rack=rack,
                    modulo_modelo=module_modelo,
                    nome=module_modelo.nome,
                )
                canais = [
                    CanalRackIO(
                        modulo=modulo,
                        indice=index,
                        nome=f"Canal {index:02d}",
                        tipo=module_modelo.tipo_base,
                    )
                    for index in range(1, module_modelo.quantidade_canais + 1)
                ]
                CanalRackIO.objects.bulk_create(canais)
                slot.modulo = modulo
                slot.save(update_fields=["modulo"])
            return redirect("ios_rack_detail", pk=rack.pk)
        if action == "remove_from_slot":
            slot_id = request.POST.get("slot_id")
            slot = get_object_or_404(RackSlotIO, pk=slot_id, rack=rack)
            if slot.modulo_id:
                slot.modulo.delete()
                slot.modulo = None
                slot.save(update_fields=["modulo"])
            return redirect("ios_rack_detail", pk=rack.pk)
        if action in ["move_left", "move_right"]:
            slot_id = request.POST.get("slot_id")
            slot = get_object_or_404(RackSlotIO, pk=slot_id, rack=rack)
            delta = -1 if action == "move_left" else 1
            neighbor = RackSlotIO.objects.filter(rack=rack, posicao=slot.posicao + delta).first()
            if neighbor:
                slot.modulo, neighbor.modulo = neighbor.modulo, slot.modulo
                slot.save(update_fields=["modulo"])
                neighbor.save(update_fields=["modulo"])
            return redirect("ios_rack_detail", pk=rack.pk)

    slots = rack.slots.select_related("modulo", "modulo__modulo_modelo").order_by("posicao")
    modules = (
        ModuloIO.objects.filter(Q(cliente=rack.cliente) | Q(is_default=True))
        .select_related("tipo_base")
        .order_by("nome")
    )
    available_qs = (
        CanalRackIO.objects.filter(modulo__rack=rack)
        .filter(Q(nome__isnull=True) | Q(nome__exact=""))
        .values("tipo__nome")
        .annotate(total=Count("id"))
        .order_by("tipo__nome")
    )
    canais_disponiveis = [
        {"tipo": row["tipo__nome"], "total": row["total"]} for row in available_qs
    ]
    ocupados = rack.slots.filter(modulo__isnull=False).count()
    slots_livres = max(rack.slots_total - ocupados, 0)
    return render(
        request,
        "core/ios_rack_detail.html",
        {
            "rack": rack,
            "slots": slots,
            "modules": modules,
            "ocupados": ocupados,
            "slots_livres": slots_livres,
            "canais_disponiveis": canais_disponiveis,
            "message": message,
        },
    )


@login_required
def ios_rack_io_list(request, pk):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    if cliente:
        rack = get_object_or_404(
            RackIO,
            Q(pk=pk),
            Q(cliente=cliente) | Q(id_planta__in=cliente.plantas.all()),
        )
    else:
        rack = get_object_or_404(RackIO, pk=pk)
    slot_pos_subquery = RackSlotIO.objects.filter(modulo_id=OuterRef("modulo_id")).values("posicao")[:1]
    canais = (
        CanalRackIO.objects.filter(modulo__rack=rack)
        .select_related("modulo", "modulo__modulo_modelo", "tipo")
        .annotate(slot_pos=Subquery(slot_pos_subquery))
        .order_by("slot_pos", "indice")
    )
    return render(
        request,
        "core/ios_rack_io_list.html",
        {
            "rack": rack,
            "canais": canais,
        },
    )


@login_required
def ios_modulos(request):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_module":
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
            nome = request.POST.get("nome", "").strip()
            modelo = request.POST.get("modelo", "").strip()
            marca = request.POST.get("marca", "").strip()
            canais_raw = request.POST.get("quantidade_canais", "").strip()
            tipo_id = request.POST.get("tipo_base")
            try:
                quantidade_canais = int(canais_raw)
            except (TypeError, ValueError):
                quantidade_canais = None
            if quantidade_canais is not None:
                quantidade_canais = max(1, min(512, quantidade_canais))
            if nome and quantidade_canais and tipo_id:
                tipo_base = get_object_or_404(TipoCanalIO, pk=tipo_id)
                modulo = ModuloIO.objects.create(
                    cliente=cliente,
                    nome=nome,
                    modelo=modelo,
                    marca=marca,
                    quantidade_canais=quantidade_canais,
                    tipo_base=tipo_base,
                )
            return redirect("ios_modulos")

    if not cliente:
        modules = ModuloIO.objects.none()
    else:
        modules = ModuloIO.objects.filter(cliente=cliente, is_default=False).select_related("tipo_base")
    channel_types = TipoCanalIO.objects.filter(ativo=True).order_by("nome")
    return render(
        request,
        "core/ios_modulos.html",
        {
            "modules": modules,
            "channel_types": channel_types,
            "can_manage": bool(cliente),
        },
    )


@login_required
def ios_modulo_modelo_detail(request, pk):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    module_qs = ModuloIO.objects.select_related("tipo_base")
    module = get_object_or_404(module_qs, pk=pk, cliente=cliente) if cliente else get_object_or_404(module_qs, pk=pk)
    if module.is_default and not request.user.is_staff:
        return HttpResponseForbidden("Sem permissao.")
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "update_model":
            nome = request.POST.get("nome", "").strip()
            modelo = request.POST.get("modelo", "").strip()
            marca = request.POST.get("marca", "").strip()
            canais_raw = request.POST.get("quantidade_canais", "").strip()
            tipo_id = request.POST.get("tipo_base")
            try:
                quantidade_canais = int(canais_raw)
            except (TypeError, ValueError):
                quantidade_canais = module.quantidade_canais
            quantidade_canais = max(1, min(512, quantidade_canais))
            if nome:
                module.nome = nome
            module.modelo = modelo
            module.marca = marca
            if tipo_id:
                module.tipo_base_id = tipo_id
            module.quantidade_canais = quantidade_canais
            module.save(update_fields=["nome", "modelo", "marca", "tipo_base", "quantidade_canais"])
            return redirect("ios_modulo_modelo_detail", pk=module.pk)
        if action == "delete_model":
            if not module.instancias.exists():
                module.delete()
                return redirect("ios_modulos")
            return redirect("ios_modulo_modelo_detail", pk=module.pk)

    channel_types = TipoCanalIO.objects.filter(ativo=True).order_by("nome")
    return render(
        request,
        "core/ios_modulo_modelo_detail.html",
        {
            "module": module,
            "channel_types": channel_types,
        },
    )


@login_required
def inventarios_list(request):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_inventario":
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            responsavel = request.POST.get("responsavel", "").strip()
            cidade = request.POST.get("cidade", "").strip()
            estado = request.POST.get("estado", "").strip()
            pais = request.POST.get("pais", "").strip()
            id_inventario_raw = request.POST.get("id_inventario", "").strip()
            id_inventario = None
            if id_inventario_raw:
                id_inventario, _ = InventarioID.objects.get_or_create(codigo=id_inventario_raw.upper())
            if nome:
                Inventario.objects.create(
                    cliente=cliente,
                    nome=nome,
                    descricao=descricao,
                    responsavel=responsavel,
                    cidade=cidade,
                    estado=estado,
                    pais=pais,
                    id_inventario=id_inventario,
                    criador=request.user,
                )
            return redirect("inventarios_list")

    if request.user.is_staff and not cliente:
        inventarios = Inventario.objects.all()
    else:
        inventarios = Inventario.objects.filter(
            Q(cliente=cliente) | Q(id_inventario__in=cliente.inventarios.all())
        )
    inventarios = inventarios.annotate(total_ativos=Count("ativos"))
    return render(
        request,
        "core/inventarios_list.html",
        {
            "inventarios": inventarios,
            "can_manage": bool(cliente),
        },
    )


@login_required
def inventario_detail(request, pk):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    if cliente:
        inventario = get_object_or_404(
            Inventario,
            Q(pk=pk),
            Q(cliente=cliente) | Q(id_inventario__in=cliente.inventarios.all()),
        )
    else:
        inventario = get_object_or_404(Inventario, pk=pk)
    message = None
    tipo_choices = Ativo.Tipo.choices
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_ativo":
            nome = request.POST.get("nome", "").strip()
            setor = request.POST.get("setor", "").strip()
            tipo = request.POST.get("tipo", "").strip()
            identificacao = request.POST.get("identificacao", "").strip()
            tag_interna = request.POST.get("tag_interna", "").strip()
            tag_set = request.POST.get("tag_set", "").strip()
            comissionado = request.POST.get("comissionado") == "on"
            em_manutencao = request.POST.get("em_manutencao") == "on"
            if nome:
                ativo = Ativo.objects.create(
                    inventario=inventario,
                    setor=setor,
                    nome=nome,
                    tipo=tipo,
                    identificacao=identificacao,
                    tag_interna=tag_interna,
                    tag_set=tag_set,
                    comissionado=comissionado,
                    em_manutencao=em_manutencao,
                )
                if comissionado:
                    ativo.comissionado_em = timezone.now()
                    ativo.comissionado_por = request.user
                if em_manutencao:
                    ativo.manutencao_em = timezone.now()
                    ativo.manutencao_por = request.user
                if comissionado or em_manutencao:
                    ativo.save(
                        update_fields=[
                            "comissionado_em",
                            "comissionado_por",
                            "manutencao_em",
                            "manutencao_por",
                        ]
                    )
            return redirect("inventario_detail", pk=inventario.pk)
        if action == "toggle_comissionado":
            ativo_id = request.POST.get("ativo_id")
            ativo = get_object_or_404(Ativo, pk=ativo_id, inventario=inventario)
            if ativo.comissionado:
                ativo.comissionado = False
                ativo.comissionado_em = None
                ativo.comissionado_por = None
            else:
                ativo.comissionado = True
                ativo.comissionado_em = timezone.now()
                ativo.comissionado_por = request.user
            ativo.save(update_fields=["comissionado", "comissionado_em", "comissionado_por"])
            return redirect("inventario_detail", pk=inventario.pk)
        if action == "toggle_manutencao":
            ativo_id = request.POST.get("ativo_id")
            ativo = get_object_or_404(Ativo, pk=ativo_id, inventario=inventario)
            if ativo.em_manutencao:
                ativo.em_manutencao = False
                ativo.manutencao_em = None
                ativo.manutencao_por = None
            else:
                ativo.em_manutencao = True
                ativo.manutencao_em = timezone.now()
                ativo.manutencao_por = request.user
            ativo.save(update_fields=["em_manutencao", "manutencao_em", "manutencao_por"])
            return redirect("inventario_detail", pk=inventario.pk)

    ativos = (
        Ativo.objects.filter(inventario=inventario, pai__isnull=True)
        .select_related("pai", "comissionado_por", "manutencao_por")
        .annotate(subativos_total=Count("subativos"))
        .order_by("nome")
    )
    total_ativos = Ativo.objects.filter(inventario=inventario).count()
    return render(
        request,
        "core/inventario_detail.html",
        {
            "inventario": inventario,
            "ativos": ativos,
            "total_ativos": total_ativos,
            "message": message,
            "tipo_choices": tipo_choices,
        },
    )


@login_required
def inventario_ativo_detail(request, inventario_pk, pk):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    if cliente:
        inventario = get_object_or_404(
            Inventario,
            Q(pk=inventario_pk),
            Q(cliente=cliente) | Q(id_inventario__in=cliente.inventarios.all()),
        )
    else:
        inventario = get_object_or_404(Inventario, pk=inventario_pk)
    ativo = get_object_or_404(
        Ativo.objects.select_related("inventario", "comissionado_por", "manutencao_por"),
        pk=pk,
        inventario=inventario,
    )
    tipo_choices = Ativo.Tipo.choices
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_subativo":
            nome = request.POST.get("nome", "").strip()
            setor = request.POST.get("setor", "").strip()
            tipo = request.POST.get("tipo", "").strip()
            identificacao = request.POST.get("identificacao", "").strip()
            tag_interna = request.POST.get("tag_interna", "").strip()
            tag_set = request.POST.get("tag_set", "").strip()
            comissionado = request.POST.get("comissionado") == "on"
            em_manutencao = request.POST.get("em_manutencao") == "on"
            if nome:
                subativo = Ativo.objects.create(
                    inventario=inventario,
                    pai=ativo,
                    setor=setor,
                    nome=nome,
                    tipo=tipo,
                    identificacao=identificacao,
                    tag_interna=tag_interna,
                    tag_set=tag_set,
                    comissionado=comissionado,
                    em_manutencao=em_manutencao,
                )
                if comissionado:
                    subativo.comissionado_em = timezone.now()
                    subativo.comissionado_por = request.user
                if em_manutencao:
                    subativo.manutencao_em = timezone.now()
                    subativo.manutencao_por = request.user
                if comissionado or em_manutencao:
                    subativo.save(
                        update_fields=[
                            "comissionado_em",
                            "comissionado_por",
                            "manutencao_em",
                            "manutencao_por",
                        ]
                    )
            return redirect("inventario_ativo_detail", inventario_pk=inventario.pk, pk=ativo.pk)
        if action == "toggle_comissionado":
            ativo_id = request.POST.get("ativo_id")
            alvo = get_object_or_404(Ativo, pk=ativo_id, inventario=inventario)
            if alvo.comissionado:
                alvo.comissionado = False
                alvo.comissionado_em = None
                alvo.comissionado_por = None
            else:
                alvo.comissionado = True
                alvo.comissionado_em = timezone.now()
                alvo.comissionado_por = request.user
            alvo.save(update_fields=["comissionado", "comissionado_em", "comissionado_por"])
            return redirect("inventario_ativo_detail", inventario_pk=inventario.pk, pk=ativo.pk)
        if action == "toggle_manutencao":
            ativo_id = request.POST.get("ativo_id")
            alvo = get_object_or_404(Ativo, pk=ativo_id, inventario=inventario)
            if alvo.em_manutencao:
                alvo.em_manutencao = False
                alvo.manutencao_em = None
                alvo.manutencao_por = None
            else:
                alvo.em_manutencao = True
                alvo.manutencao_em = timezone.now()
                alvo.manutencao_por = request.user
            alvo.save(update_fields=["em_manutencao", "manutencao_em", "manutencao_por"])
            return redirect("inventario_ativo_detail", inventario_pk=inventario.pk, pk=ativo.pk)

    subativos = (
        Ativo.objects.filter(inventario=inventario, pai=ativo)
        .select_related("comissionado_por", "manutencao_por")
        .annotate(subativos_total=Count("subativos"))
        .order_by("nome")
    )
    return render(
        request,
        "core/inventario_ativo_detail.html",
        {
            "inventario": inventario,
            "ativo": ativo,
            "subativos": subativos,
            "tipo_choices": tipo_choices,
        },
    )


@login_required
def ios_rack_modulo_detail(request, pk):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    module_qs = ModuloRackIO.objects.select_related("modulo_modelo", "rack")
    if cliente:
        module = get_object_or_404(
            module_qs,
            Q(pk=pk),
            Q(rack__cliente=cliente) | Q(rack__id_planta__in=cliente.plantas.all()),
        )
    else:
        module = get_object_or_404(module_qs, pk=pk)
    slot = RackSlotIO.objects.filter(modulo=module).select_related("rack").first()
    prev_slot = None
    next_slot = None
    if slot:
        prev_slot = (
            RackSlotIO.objects.filter(rack=module.rack, modulo__isnull=False, posicao__lt=slot.posicao)
            .select_related("modulo")
            .order_by("-posicao")
            .first()
        )
        next_slot = (
            RackSlotIO.objects.filter(rack=module.rack, modulo__isnull=False, posicao__gt=slot.posicao)
            .select_related("modulo")
            .order_by("posicao")
            .first()
        )
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "update_module_name":
            nome = request.POST.get("nome", "").strip()
            module.nome = nome
            module.save(update_fields=["nome"])
            return redirect("ios_rack_modulo_detail", pk=module.pk)
        if action == "update_module":
            nome = request.POST.get("nome", "").strip()
            if nome:
                module.nome = nome
                module.save(update_fields=["nome"])
            if request.POST.get("delete_module") == "on":
                rack_id = module.rack_id
                module.delete()
                return redirect("ios_rack_detail", pk=rack_id)
            target_slot_id = request.POST.get("slot_id")
            if target_slot_id:
                target_slot = get_object_or_404(RackSlotIO, pk=target_slot_id, rack=module.rack)
                if not target_slot.modulo_id:
                    if slot:
                        slot.modulo = None
                        slot.save(update_fields=["modulo"])
                    target_slot.modulo = module
                    target_slot.save(update_fields=["modulo"])
            return redirect("ios_rack_modulo_detail", pk=module.pk)
        if action == "move_to_slot":
            target_slot_id = request.POST.get("slot_id")
            target_slot = get_object_or_404(RackSlotIO, pk=target_slot_id, rack=module.rack)
            if target_slot.modulo_id:
                return redirect("ios_rack_modulo_detail", pk=module.pk)
            if slot:
                slot.modulo = None
                slot.save(update_fields=["modulo"])
            target_slot.modulo = module
            target_slot.save(update_fields=["modulo"])
            return redirect("ios_rack_modulo_detail", pk=module.pk)
        if action == "delete_module":
            rack_id = module.rack_id
            module.delete()
            return redirect("ios_rack_detail", pk=rack_id)
        if action == "update_channels":
            for channel in module.canais.all():
                nome_raw = request.POST.get(f"nome_{channel.id}")
                tipo_id = request.POST.get(f"tipo_{channel.id}")
                if nome_raw is None:
                    continue
                channel.nome = nome_raw.strip()
                if tipo_id:
                    channel.tipo_id = tipo_id
                channel.save(update_fields=["nome", "tipo_id"])
            return redirect("ios_rack_modulo_detail", pk=module.pk)
    channels = module.canais.select_related("tipo").order_by("indice")
    channel_types = TipoCanalIO.objects.filter(ativo=True).order_by("nome")
    vacant_slots = RackSlotIO.objects.filter(rack=module.rack, modulo__isnull=True).order_by("posicao")
    return render(
        request,
        "core/ios_modulo_detail.html",
        {
            "module": module,
            "channels": channels,
            "channel_types": channel_types,
            "rack": module.rack,
            "slot": slot,
            "vacant_slots": vacant_slots,
            "has_vacant_slots": vacant_slots.exists(),
            "prev_slot": prev_slot,
            "next_slot": next_slot,
        },
    )


@login_required
def proposta_list(request):
    cliente = _get_cliente(request.user)
    propostas = Proposta.objects.none()
    if cliente:
        propostas = (
            Proposta.objects.filter(Q(criada_por=request.user) | Q(cliente=cliente))
            .distinct()
            .order_by("-criado_em")
        )
    else:
        propostas = Proposta.objects.filter(criada_por=request.user).order_by("-criado_em")
    status = request.GET.get("status")
    if status == "pendente":
        propostas = propostas.filter(aprovada__isnull=True).exclude(valor=0)
    elif status == "levantamento":
        propostas = propostas.filter(aprovada__isnull=True, valor=0)
    elif status == "aprovada":
        propostas = propostas.filter(aprovada=True)
    elif status == "reprovada":
        propostas = propostas.filter(aprovada=False)
    elif status == "finalizada":
        propostas = propostas.filter(finalizada=True)
    else:
        propostas = propostas.exclude(aprovada=False).exclude(finalizada=True)
    propostas = propostas.annotate(
        status_order=Case(
            When(finalizada=True, then=Value(6)),
            When(andamento="EXECUTANDO", then=Value(4)),
            When(aprovada=True, then=Value(3)),
            When(aprovada=False, then=Value(5)),
            When(aprovada__isnull=True, valor=0, then=Value(1)),
            default=Value(2),
            output_field=IntegerField(),
        )
    ).order_by("status_order", "-criado_em")
    propostas = propostas.annotate(
        status_label=Case(
            When(andamento="EXECUTANDO", then=Value("Executando")),
            When(aprovada__isnull=True, valor=0, then=Value("Levantamento")),
            When(aprovada=True, then=Value("Aprovada")),
            When(aprovada=False, then=Value("Reprovada")),
            default=Value("Pendente"),
        )
    )
    propostas_para_aprovar = Proposta.objects.none()
    propostas_restantes = propostas
    if not status:
        propostas_para_aprovar = propostas.filter(
            aprovada__isnull=True,
            valor__gt=0,
            cliente__usuario=request.user,
        )
        propostas_restantes = propostas.exclude(pk__in=propostas_para_aprovar.values("pk"))
    pendencias_total = propostas_para_aprovar.count()
    return render(
        request,
        "core/proposta_list.html",
        {
            "cliente": cliente,
            "propostas": propostas,
            "propostas_para_aprovar": propostas_para_aprovar,
            "propostas_restantes": propostas_restantes,
            "pendencias_total": pendencias_total,
            "status_filter": status,
            "is_vendedor": True,
            "current_user_id": request.user.id,
        },
    )


@login_required
def proposta_detail(request, pk):
    cliente = _get_cliente(request.user)
    if cliente:
        proposta_qs = Proposta.objects.filter(Q(criada_por=request.user) | Q(cliente=cliente))
        proposta = get_object_or_404(proposta_qs, pk=pk)
    else:
        proposta = get_object_or_404(Proposta, pk=pk, criada_por=request.user)
    message = None
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "update_value":
            if proposta.criada_por_id != request.user.id:
                return HttpResponseForbidden("Sem permissao.")
            if proposta.aprovada is not None:
                message = "Nao e possivel alterar o valor apos aprovacao."
            else:
                valor_raw = request.POST.get("valor", "").replace(",", ".").strip()
                try:
                    valor = Decimal(valor_raw)
                except (InvalidOperation, ValueError):
                    valor = None
                if valor is None:
                    message = "Informe um valor valido."
                else:
                    proposta.valor = valor
                    proposta.save(update_fields=["valor"])
                    return redirect("proposta_detail", pk=proposta.pk)
        if action == "update_details":
            if proposta.criada_por_id != request.user.id:
                return HttpResponseForbidden("Sem permissao.")
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            codigo = request.POST.get("codigo", "").strip()
            update_fields = []
            if nome:
                proposta.nome = nome
                update_fields.append("nome")
            if descricao:
                proposta.descricao = descricao
                update_fields.append("descricao")
            if codigo:
                proposta.codigo = codigo
                update_fields.append("codigo")
            if update_fields:
                proposta.save(update_fields=update_fields)
            return redirect("proposta_detail", pk=proposta.pk)
        if action == "set_finalizada":
            if proposta.criada_por_id != request.user.id:
                return HttpResponseForbidden("Sem permissao.")
            if proposta.aprovada is None:
                message = "Aguardando aprovacao. Finalizacao so e possivel apos aprovacao."
            else:
                proposta.finalizada = True
                proposta.save(update_fields=["finalizada"])
                return redirect("proposta_detail", pk=proposta.pk)
        if action == "set_executando":
            if proposta.criada_por_id != request.user.id:
                return HttpResponseForbidden("Sem permissao.")
            if proposta.aprovada is not True:
                message = "Somente propostas aprovadas podem ir para Executando."
            else:
                proposta.andamento = "EXECUTANDO"
                proposta.save(update_fields=["andamento"])
                return redirect("proposta_detail", pk=proposta.pk)
        if action == "add_anexo":
            arquivo = request.FILES.get("arquivo")
            tipo = request.POST.get("tipo") or PropostaAnexo.Tipo.OUTROS
            if arquivo:
                PropostaAnexo.objects.create(proposta=proposta, arquivo=arquivo, tipo=tipo)
            return redirect("proposta_detail", pk=proposta.pk)
        if action == "delete_anexo":
            anexo_id = request.POST.get("anexo_id")
            anexo = get_object_or_404(PropostaAnexo, pk=anexo_id, proposta=proposta)
            anexo.delete()
            return redirect("proposta_detail", pk=proposta.pk)
        if action == "delete_proposta":
            if proposta.criada_por_id != request.user.id:
                return HttpResponseForbidden("Sem permissao.")
            if proposta.aprovada is not None:
                message = "Nao e possivel excluir apos aprovacao."
            else:
                proposta.delete()
                return redirect("propostas")
    if proposta.finalizada:
        status_label = "Finalizada"
    elif proposta.andamento == "EXECUTANDO":
        status_label = "Executando"
    elif proposta.aprovada is True:
        status_label = "Aprovada"
    elif proposta.aprovada is False:
        status_label = "Reprovada"
    elif proposta.valor == 0:
        status_label = "Levantamento"
    else:
        status_label = "Pendente"
    return render(
        request,
        "core/proposta_detail.html",
        {
            "cliente": cliente,
            "proposta": proposta,
            "message": message,
            "status_label": status_label,
        },
    )


@login_required
def proposta_nova_vendedor(request):
    message = None
    form_data = {"email": "", "nome": "", "descricao": "", "valor": "", "prioridade": "50"}
    if request.method == "POST":
        email = request.POST.get("email", "").strip().lower()
        nome = request.POST.get("nome", "").strip()
        descricao = request.POST.get("descricao", "").strip()
        valor_raw = request.POST.get("valor", "").replace(",", ".").strip()
        prioridade_raw = request.POST.get("prioridade", "").strip()
        form_data = {
            "email": email,
            "nome": nome,
            "descricao": descricao,
            "valor": valor_raw,
            "prioridade": prioridade_raw or "50",
        }

        destinatario = PerfilUsuario.objects.filter(email__iexact=email).first() if email else None
        if not destinatario:
            message = "Usuario nao encontrado para este email."
        else:
            try:
                valor = Decimal(valor_raw)
            except (InvalidOperation, ValueError):
                valor = None
            try:
                prioridade = int(prioridade_raw) if prioridade_raw else 50
            except ValueError:
                prioridade = 50
            prioridade = max(1, min(99, prioridade))
            if not nome or not descricao or valor is None:
                message = "Preencha nome, descricao e valor valido."
            else:
                proposta = Proposta.objects.create(
                    cliente=destinatario,
                    criada_por=request.user,
                    nome=nome,
                    descricao=descricao,
                    valor=valor,
                    prioridade=prioridade,
                )
                return redirect("proposta_detail", pk=proposta.pk)

    return render(
        request,
        "core/proposta_nova.html",
        {
            "message": message,
            "form_data": form_data,
        },
    )


@login_required
@require_POST
def aprovar_proposta(request, pk):
    cliente = _get_cliente(request.user)
    proposta = get_object_or_404(Proposta, pk=pk, cliente=cliente)
    if proposta.cliente.usuario_id != request.user.id:
        return HttpResponseForbidden("Somente o destinatario pode aprovar.")
    if proposta.valor == 0:
        return HttpResponseForbidden("Proposta em levantamento.")
    if proposta.aprovada is None:
        proposta.aprovada = True
        proposta.decidido_em = timezone.now()
        proposta.aprovado_por = request.user
        proposta.save(update_fields=["aprovada", "decidido_em", "aprovado_por"])
    return redirect("propostas")


@login_required
@require_POST
def reprovar_proposta(request, pk):
    cliente = _get_cliente(request.user)
    proposta = get_object_or_404(Proposta, pk=pk, cliente=cliente)
    if proposta.cliente.usuario_id != request.user.id:
        return HttpResponseForbidden("Somente o destinatario pode reprovar.")
    if proposta.aprovada is None:
        proposta.aprovada = False
        proposta.decidido_em = timezone.now()
        proposta.aprovado_por = request.user
        proposta.save(update_fields=["aprovada", "decidido_em", "aprovado_por"])
    return redirect("propostas")


@login_required
@require_POST
def salvar_observacao(request, pk):
    cliente = _get_cliente(request.user)
    if cliente:
        proposta_qs = Proposta.objects.filter(Q(criada_por=request.user) | Q(cliente=cliente))
        proposta = get_object_or_404(proposta_qs, pk=pk)
    else:
        proposta = get_object_or_404(Proposta, pk=pk, criada_por=request.user)
    observacao = request.POST.get("observacao", "").strip()
    prioridade_raw = request.POST.get("prioridade", "").strip()
    proposta.observacao_cliente = observacao
    if prioridade_raw:
        try:
            prioridade = int(prioridade_raw)
        except ValueError:
            prioridade = None
        if prioridade is not None:
            prioridade = max(1, min(99, prioridade))
            proposta.prioridade = prioridade
    proposta.save(update_fields=["observacao_cliente", "prioridade"])
    return redirect("proposta_detail", pk=proposta.pk)


@login_required
def user_management(request):
    if not request.user.is_staff:
        return HttpResponseForbidden("Sem permissao.")
    message = None
    form = UserCreateForm()
    tipo_form = TipoPerfilCreateForm()
    if request.method == "POST":
        if request.POST.get("create_tipo") == "1":
            tipo_form = TipoPerfilCreateForm(request.POST)
            if tipo_form.is_valid():
                tipo_form.save()
                return redirect("usuarios")
        elif request.POST.get("update_tipo") == "1":
            tipo_id = request.POST.get("tipo_id")
            novo_nome = request.POST.get("novo_nome", "").strip()
            tipo = get_object_or_404(TipoPerfil, pk=tipo_id)
            if novo_nome:
                if TipoPerfil.objects.exclude(pk=tipo.id).filter(nome__iexact=novo_nome).exists():
                    message = "Tipo ja existe."
                else:
                    tipo.nome = novo_nome
                    tipo.save(update_fields=["nome"])
                    return redirect("usuarios")
            else:
                message = "Informe um nome valido."
        elif request.POST.get("delete_tipo") == "1":
            tipo_id = request.POST.get("tipo_id")
            tipo = get_object_or_404(TipoPerfil, pk=tipo_id)
            tipo.delete()
            return redirect("usuarios")
        if request.POST.get("create_user") == "1":
            form = UserCreateForm(request.POST)
            if form.is_valid():
                user = form.save()
                tipo_ids = request.POST.getlist("tipos")
                tipos = TipoPerfil.objects.filter(id__in=tipo_ids) if tipo_ids else TipoPerfil.objects.none()
                nome = user.username.split("@")[0]
                cliente = PerfilUsuario.objects.create(
                    nome=nome,
                    email=user.username,
                    usuario=user,
                    ativo=True,
                )
                if tipos:
                    cliente.tipos.set(tipos)
                _ensure_default_cadernos(cliente)
                return redirect("usuarios")
    else:
        form = UserCreateForm()
    users = User.objects.order_by("username")
    return render(
        request,
        "core/usuarios.html",
        {
            "form": form,
            "users": users,
            "tipos": TipoPerfil.objects.order_by("nome"),
            "tipo_form": tipo_form,
            "message": message,
        },
    )


@login_required
def usuarios_gerenciar_usuario(request, pk):
    if not request.user.is_staff:
        return HttpResponseForbidden("Sem permissao.")
    user = get_object_or_404(User, pk=pk)
    perfil = _get_cliente(user)
    message = None
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "update_user":
            email = request.POST.get("email", "").strip().lower()
            is_staff = request.POST.get("is_staff") == "on"
            is_active = request.POST.get("is_active") == "on"
            if not email:
                message = "Informe um email valido."
            else:
                existing = User.objects.filter(username=email).exclude(pk=user.pk)
                if existing.exists():
                    message = "Email ja cadastrado."
                else:
                    user.username = email
                    user.email = email
                    user.is_staff = is_staff
                    user.is_active = is_active
                    user.save(update_fields=["username", "email", "is_staff", "is_active"])
                    if perfil:
                        perfil.email = email
                        perfil.save(update_fields=["email"])
                    return redirect("usuarios_gerenciar_usuario", pk=user.pk)
        if action == "update_perfil":
            nome = request.POST.get("nome", "").strip()
            empresa = request.POST.get("empresa", "").strip()
            sigla_cidade = request.POST.get("sigla_cidade", "").strip()
            tipo_ids = request.POST.getlist("tipos")
            plantas_raw = request.POST.get("plantas", "")
            financeiros_raw = request.POST.get("financeiros", "")
            inventarios_raw = request.POST.get("inventarios", "")
            if not perfil:
                perfil = PerfilUsuario.objects.create(
                    nome=nome or user.username.split("@")[0],
                    email=user.email or user.username,
                    usuario=user,
                    ativo=True,
                    empresa=empresa,
                    sigla_cidade=sigla_cidade,
                )
                _ensure_default_cadernos(perfil)
            else:
                if nome:
                    perfil.nome = nome
                perfil.empresa = empresa
                perfil.sigla_cidade = sigla_cidade
                perfil.save(update_fields=["nome", "empresa", "sigla_cidade"])
            tipos = TipoPerfil.objects.filter(id__in=tipo_ids)
            perfil.tipos.set(tipos)
            cleaned = plantas_raw
            for sep in [";", "\n", "\r", "\t"]:
                cleaned = cleaned.replace(sep, ",")
            codes = [code.strip().upper() for code in cleaned.split(",") if code.strip()]
            plantas = [PlantaIO.objects.get_or_create(codigo=code)[0] for code in codes]
            perfil.plantas.set(plantas)
            cleaned_fin = financeiros_raw
            for sep in [";", "\n", "\r", "\t"]:
                cleaned_fin = cleaned_fin.replace(sep, ",")
            fin_codes = [code.strip().upper() for code in cleaned_fin.split(",") if code.strip()]
            financeiros = [FinanceiroID.objects.get_or_create(codigo=code)[0] for code in fin_codes]
            perfil.financeiros.set(financeiros)
            cleaned_inv = inventarios_raw
            for sep in [";", "\n", "\r", "\t"]:
                cleaned_inv = cleaned_inv.replace(sep, ",")
            inv_codes = [code.strip().upper() for code in cleaned_inv.split(",") if code.strip()]
            inventarios = [InventarioID.objects.get_or_create(codigo=code)[0] for code in inv_codes]
            perfil.inventarios.set(inventarios)
            return redirect("usuarios_gerenciar_usuario", pk=user.pk)
        if action == "set_password":
            new_password = request.POST.get("new_password", "").strip()
            if new_password:
                user.set_password(new_password)
                user.save(update_fields=["password"])
                message = "Senha atualizada."
            else:
                message = "Informe uma senha valida."
    return render(
        request,
        "core/usuarios_gerenciar_usuario.html",
        {
            "user_item": user,
            "perfil": perfil,
            "tipos": TipoPerfil.objects.order_by("nome"),
            "message": message,
        },
    )


@login_required
def meu_perfil(request):
    user = request.user
    perfil = _get_cliente(user)
    message = None
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "update_user":
            email = request.POST.get("email", "").strip().lower()
            if not email:
                message = "Informe um email valido."
            else:
                existing = User.objects.filter(username=email).exclude(pk=user.pk)
                if existing.exists():
                    message = "Email ja cadastrado."
                else:
                    user.username = email
                    user.email = email
                    user.save(update_fields=["username", "email"])
                    if perfil:
                        perfil.email = email
                        perfil.save(update_fields=["email"])
                    return redirect("meu_perfil")
        if action == "update_profile":
            nome = request.POST.get("nome", "").strip()
            empresa = request.POST.get("empresa", "").strip()
            sigla_cidade = request.POST.get("sigla_cidade", "").strip()
            plantas_raw = request.POST.get("plantas", "")
            financeiros_raw = request.POST.get("financeiros", "")
            inventarios_raw = request.POST.get("inventarios", "")
            if not perfil:
                perfil = PerfilUsuario.objects.create(
                    nome=nome or (user.username.split("@")[0] if user.username else "Usuario"),
                    email=user.email or user.username,
                    usuario=user,
                    ativo=True,
                    empresa=empresa,
                    sigla_cidade=sigla_cidade,
                )
                _ensure_default_cadernos(perfil)
            else:
                if nome:
                    perfil.nome = nome
                perfil.empresa = empresa
                perfil.sigla_cidade = sigla_cidade
                perfil.save(update_fields=["nome", "empresa", "sigla_cidade"])
            cleaned = plantas_raw
            for sep in [";", "\n", "\r", "\t"]:
                cleaned = cleaned.replace(sep, ",")
            codes = [code.strip().upper() for code in cleaned.split(",") if code.strip()]
            plantas = [PlantaIO.objects.get_or_create(codigo=code)[0] for code in codes]
            perfil.plantas.set(plantas)
            cleaned_fin = financeiros_raw
            for sep in [";", "\n", "\r", "\t"]:
                cleaned_fin = cleaned_fin.replace(sep, ",")
            fin_codes = [code.strip().upper() for code in cleaned_fin.split(",") if code.strip()]
            financeiros = [FinanceiroID.objects.get_or_create(codigo=code)[0] for code in fin_codes]
            perfil.financeiros.set(financeiros)
            cleaned_inv = inventarios_raw
            for sep in [";", "\n", "\r", "\t"]:
                cleaned_inv = cleaned_inv.replace(sep, ",")
            inv_codes = [code.strip().upper() for code in cleaned_inv.split(",") if code.strip()]
            inventarios = [InventarioID.objects.get_or_create(codigo=code)[0] for code in inv_codes]
            perfil.inventarios.set(inventarios)
            return redirect("meu_perfil")
        if action == "set_password":
            new_password = request.POST.get("new_password", "").strip()
            if new_password:
                user.set_password(new_password)
                user.save(update_fields=["password"])
                message = "Senha atualizada."
            else:
                message = "Informe uma senha valida."
    return render(
        request,
        "core/meu_perfil.html",
        {
            "perfil": perfil,
            "message": message,
        },
    )


@login_required
def financeiro_overview(request):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    if cliente:
        cadernos = Caderno.objects.filter(Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all()))
    else:
        cadernos = Caderno.objects.none()
    total_expr = ExpressionWrapper(
        F("compras__itens__valor") * F("compras__itens__quantidade"),
        output_field=DecimalField(max_digits=12, decimal_places=2),
    )
    today = timezone.localdate()
    start_date = date(today.year, today.month, 1)
    if today.month == 12:
        end_date = date(today.year + 1, 1, 1)
    else:
        end_date = date(today.year, today.month + 1, 1)
    item_expr = ExpressionWrapper(
        F("itens__valor") * F("itens__quantidade"),
        output_field=DecimalField(max_digits=12, decimal_places=2),
    )
    cadernos = cadernos.annotate(
        total_mes=Sum(
            total_expr,
            filter=Q(compras__data__gte=start_date, compras__data__lt=end_date),
        )
    ).order_by("nome")
    if cliente:
        compras_qs = Compra.objects.filter(
            Q(caderno__criador=cliente) | Q(caderno__id_financeiro__in=cliente.financeiros.all())
        )
        total_geral = compras_qs.aggregate(total=Sum(item_expr)).get("total")
        ultimas_compras = compras_qs.prefetch_related("itens").order_by("-data")[:6]
    else:
        total_geral = None
        ultimas_compras = Compra.objects.none()

    caderno_id = request.GET.get("caderno_id")
    compras = Compra.objects.none()
    if cliente and caderno_id:
        compras = Compra.objects.filter(
            Q(caderno_id=caderno_id),
            Q(caderno__criador=cliente) | Q(caderno__id_financeiro__in=cliente.financeiros.all()),
        ).order_by("-data")
        compras = compras.prefetch_related("itens")

    return render(
        request,
        "core/financeiro_overview.html",
        {
            "cliente": cliente,
            "cadernos": cadernos,
            "total_geral": total_geral or 0,
            "compras": compras,
            "caderno_id": caderno_id,
            "ultimas_compras": ultimas_compras,
        },
    )


@login_required
def financeiro_nova(request):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")

    message = request.GET.get("msg", "").strip()
    message_level = request.GET.get("level", "").strip() or "info"
    open_cadastro = request.GET.get("cadastro", "").strip()
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_categoria":
            nome = request.POST.get("categoria_nome", "").strip()
            next_caderno_id = request.POST.get("next_caderno_id", "").strip()
            if not nome:
                msg = "Informe um nome de categoria."
                level = "error"
                created = False
            else:
                categoria, created = CategoriaCompra.objects.get_or_create(nome=nome)
                if created:
                    msg = "Categoria criada."
                    level = "success"
                else:
                    msg = "Categoria ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": categoria.id if nome and "categoria" in locals() else None,
                        "nome": categoria.nome if nome and "categoria" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            params = {"cadastro": "categoria", "msg": msg, "level": level}
            if next_caderno_id:
                params["caderno_id"] = next_caderno_id
            return redirect(f"{reverse('financeiro_nova')}?{urlencode(params)}")
        if action == "create_centro":
            nome = request.POST.get("centro_nome", "").strip()
            next_caderno_id = request.POST.get("next_caderno_id", "").strip()
            if not nome:
                msg = "Informe um nome de centro de custo."
                level = "error"
                created = False
            else:
                centro, created = CentroCusto.objects.get_or_create(nome=nome)
                if created:
                    msg = "Centro de custo criado."
                    level = "success"
                else:
                    msg = "Centro de custo ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": centro.id if nome and "centro" in locals() else None,
                        "nome": centro.nome if nome and "centro" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            params = {"cadastro": "centro", "msg": msg, "level": level}
            if next_caderno_id:
                params["caderno_id"] = next_caderno_id
            return redirect(f"{reverse('financeiro_nova')}?{urlencode(params)}")
        if action == "create_compra":
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
            caderno_id = request.POST.get("caderno")
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            data_raw = request.POST.get("data", "").strip()
            categoria_id = request.POST.get("categoria")
            centro_id = request.POST.get("centro_custo")
            itens_payload = []
            total_items_raw = request.POST.get("total_items", "1").strip()
            try:
                total_items = int(total_items_raw)
            except ValueError:
                total_items = 1
            total_items = max(1, min(total_items, 200))
            for idx in range(total_items):
                item_nome = request.POST.get(f"item_nome_{idx}", "").strip()
                item_valor = request.POST.get(f"item_valor_{idx}", "").replace(",", ".").strip()
                item_quantidade = request.POST.get(f"item_quantidade_{idx}", "").strip()
                item_tipo = request.POST.get(f"item_tipo_{idx}")
                item_pago = request.POST.get(f"item_pago_{idx}") == "on"
                if item_nome:
                    itens_payload.append(
                        {
                            "nome": item_nome,
                            "valor": item_valor,
                            "quantidade": item_quantidade,
                            "tipo_id": item_tipo,
                            "pago": item_pago,
                        }
                    )
            try:
                data = datetime.strptime(data_raw, "%Y-%m-%d").date()
            except ValueError:
                data = None
            allowed_cadernos = Caderno.objects.filter(
                Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all())
            )
            has_any = any(
                [
                    caderno_id,
                    nome,
                    descricao,
                    data is not None,
                    categoria_id,
                    centro_id,
                    itens_payload,
                ]
            )
            if has_any:
                if caderno_id and not allowed_cadernos.filter(id=caderno_id).exists():
                    return redirect("financeiro")
                compra = Compra.objects.create(
                    caderno_id=caderno_id or None,
                    nome=nome,
                    descricao=descricao,
                    data=data,
                    categoria_id=categoria_id or None,
                    centro_custo_id=centro_id or None,
                )
                for item in itens_payload:
                    valor_raw = item["valor"]
                    quantidade_raw = item["quantidade"]
                    try:
                        valor = Decimal(valor_raw) if valor_raw else None
                    except (InvalidOperation, ValueError):
                        valor = None
                    try:
                        quantidade = int(quantidade_raw) if quantidade_raw else 1
                    except ValueError:
                        quantidade = 1
                    quantidade = max(1, quantidade)
                    CompraItem.objects.create(
                        compra=compra,
                        nome=item["nome"],
                        valor=valor,
                        quantidade=quantidade,
                        tipo_id=item["tipo_id"] or None,
                        pago=item["pago"],
                    )
                return redirect("financeiro_compra_detail", pk=compra.pk)
            return redirect("financeiro")

    if cliente:
        cadernos = Caderno.objects.filter(Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all()))
    else:
        cadernos = Caderno.objects.none()
    categorias = CategoriaCompra.objects.order_by("nome")
    centros = CentroCusto.objects.order_by("nome")
    tipos = TipoCompra.objects.order_by("nome")
    selected_caderno_id = request.GET.get("caderno_id") or ""

    return render(
        request,
        "core/financeiro_nova.html",
        {
            "cliente": cliente,
            "cadernos": cadernos,
            "categorias": categorias,
            "centros": centros,
            "tipos": tipos,
            "selected_caderno_id": str(selected_caderno_id),
            "message": message,
            "message_level": message_level,
            "open_cadastro": open_cadastro,
        },
    )


@login_required
def financeiro_cadernos(request):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")

    message = request.GET.get("msg", "").strip()
    message_level = request.GET.get("level", "").strip() or "info"
    open_cadastro = request.GET.get("cadastro", "").strip()
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_caderno":
            nome = request.POST.get("nome", "").strip()
            id_financeiro_raw = request.POST.get("id_financeiro", "").strip()
            if nome and cliente:
                financeiro = None
                if id_financeiro_raw:
                    financeiro, _ = FinanceiroID.objects.get_or_create(codigo=id_financeiro_raw.upper())
                Caderno.objects.create(nome=nome, ativo=True, id_financeiro=financeiro, criador=cliente)
            return redirect("financeiro_cadernos")
        if action == "toggle_caderno":
            caderno_id = request.POST.get("caderno_id")
            caderno = get_object_or_404(
                Caderno,
                Q(pk=caderno_id),
                Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all()),
            )
            caderno.ativo = not caderno.ativo
            caderno.save(update_fields=["ativo"])
            return redirect("financeiro_cadernos")
        if action == "delete_caderno":
            caderno_id = request.POST.get("caderno_id")
            caderno = get_object_or_404(
                Caderno,
                Q(pk=caderno_id),
                Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all()),
            )
            caderno.delete()
            return redirect("financeiro_cadernos")
        if action == "create_categoria":
            nome = request.POST.get("categoria_nome", "").strip()
            if not nome:
                msg = "Informe um nome de categoria."
                level = "error"
                created = False
            else:
                categoria, created = CategoriaCompra.objects.get_or_create(nome=nome)
                if created:
                    msg = "Categoria criada."
                    level = "success"
                else:
                    msg = "Categoria ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": categoria.id if nome and "categoria" in locals() else None,
                        "nome": categoria.nome if nome and "categoria" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            params = {"cadastro": "categoria", "msg": msg, "level": level}
            return redirect(f"{reverse('financeiro_cadernos')}?{urlencode(params)}")
        if action == "create_centro":
            nome = request.POST.get("centro_nome", "").strip()
            if not nome:
                msg = "Informe um nome de centro de custo."
                level = "error"
                created = False
            else:
                centro, created = CentroCusto.objects.get_or_create(nome=nome)
                if created:
                    msg = "Centro de custo criado."
                    level = "success"
                else:
                    msg = "Centro de custo ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": centro.id if nome and "centro" in locals() else None,
                        "nome": centro.nome if nome and "centro" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            params = {"cadastro": "centro", "msg": msg, "level": level}
            return redirect(f"{reverse('financeiro_cadernos')}?{urlencode(params)}")

    total_expr = ExpressionWrapper(
        F("compras__itens__valor") * F("compras__itens__quantidade"),
        output_field=DecimalField(max_digits=12, decimal_places=2),
    )
    cadernos = (
        Caderno.objects.filter(Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all()))
        .annotate(total=Sum(total_expr))
        .order_by("nome")
    )
    return render(
        request,
        "core/financeiro_cadernos.html",
        {
            "cadernos": cadernos,
            "message": message,
            "message_level": message_level,
            "open_cadastro": open_cadastro,
        },
    )


@login_required
def financeiro_caderno_detail(request, pk):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    caderno = get_object_or_404(
        Caderno,
        Q(pk=pk),
        Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all()),
    )
    today = timezone.localdate()
    selected_month = request.GET.get("mes", "").strip()
    if selected_month:
        try:
            selected_dt = datetime.strptime(selected_month, "%Y-%m")
        except ValueError:
            selected_dt = datetime(today.year, today.month, 1)
            selected_month = selected_dt.strftime("%Y-%m")
    else:
        selected_dt = datetime(today.year, today.month, 1)
        selected_month = selected_dt.strftime("%Y-%m")
    start_date = date(selected_dt.year, selected_dt.month, 1)
    if selected_dt.month == 12:
        end_date = date(selected_dt.year + 1, 1, 1)
    else:
        end_date = date(selected_dt.year, selected_dt.month + 1, 1)

    status_filter = request.GET.get("status", "").strip().lower()
    categoria_filter = request.GET.get("categoria", "").strip()
    centro_filter = request.GET.get("centro", "").strip()

    compras_base_qs = (
        Compra.objects.filter(caderno=caderno)
        .select_related("categoria", "centro_custo")
        .prefetch_related("itens")
        .order_by("-data", "-id")
    )
    if categoria_filter:
        compras_base_qs = compras_base_qs.filter(categoria_id=categoria_filter)
    if centro_filter:
        compras_base_qs = compras_base_qs.filter(centro_custo_id=centro_filter)

    compras_qs = compras_base_qs.filter(data__gte=start_date, data__lt=end_date)
    compras_sem_data_qs = compras_base_qs.filter(data__isnull=True)

    compras = []
    compras_sem_data = []
    total_mes = Decimal(0)
    total_pago = Decimal(0)
    total_pendente = Decimal(0)
    total_compras = 0
    total_pagas = 0
    total_pendentes = 0

    for compra in compras_qs:
        itens = list(compra.itens.all())
        compra.status_label = _compra_status_label(compra)
        compra.total_itens = sum(
            (item.valor or Decimal(0)) * (item.quantidade or 0) for item in itens
        )
        compra.total_pago = sum(
            (item.valor or Decimal(0)) * (item.quantidade or 0) for item in itens if item.pago
        )
        compra.total_pendente = compra.total_itens - compra.total_pago
        compra.itens_count = len(itens)
        if status_filter == "pago" and compra.status_label.lower() != "pago":
            continue
        if status_filter == "pendente" and compra.status_label.lower() != "pendente":
            continue
        compras.append(compra)
        total_mes += compra.total_itens
        total_pago += compra.total_pago
        total_pendente += compra.total_pendente
        total_compras += 1
        if compra.status_label.lower() == "pago":
            total_pagas += 1
        else:
            total_pendentes += 1
    ticket_medio = total_mes / total_compras if total_compras else Decimal(0)

    for compra in compras_sem_data_qs:
        itens = list(compra.itens.all())
        compra.status_label = _compra_status_label(compra)
        if status_filter == "pago" and compra.status_label.lower() != "pago":
            continue
        if status_filter == "pendente" and compra.status_label.lower() != "pendente":
            continue
        compra.total_itens = sum(
            (item.valor or Decimal(0)) * (item.quantidade or 0) for item in itens
        )
        compra.total_pago = sum(
            (item.valor or Decimal(0)) * (item.quantidade or 0) for item in itens if item.pago
        )
        compra.total_pendente = compra.total_itens - compra.total_pago
        compra.itens_count = len(itens)
        compras_sem_data.append(compra)

    categorias = CategoriaCompra.objects.order_by("nome")
    centros = CentroCusto.objects.order_by("nome")
    search_query = request.GET.get("q", "").strip()
    if search_query:
        compras = [
            compra
            for compra in compras
            if search_query.lower()
            in (
                (compra.nome or compra.descricao or "")
                .strip()
                .lower()
            )
        ]
        compras_sem_data = [
            compra
            for compra in compras_sem_data
            if search_query.lower()
            in (
                (compra.nome or compra.descricao or "")
                .strip()
                .lower()
            )
        ]
    return render(
        request,
        "core/financeiro_caderno_detail.html",
        {
            "caderno": caderno,
            "compras": compras,
            "compras_sem_data": compras_sem_data,
            "selected_month": selected_month,
            "mes_referencia": start_date,
            "status_filter": status_filter,
            "categoria_filter": categoria_filter,
            "centro_filter": centro_filter,
            "search_query": search_query,
            "categorias": categorias,
            "centros": centros,
            "resumo": {
                "total_mes": total_mes,
                "total_pago": total_pago,
                "total_pendente": total_pendente,
                "total_compras": total_compras,
                "total_pagas": total_pagas,
                "total_pendentes": total_pendentes,
                "ticket_medio": ticket_medio,
            },
        },
    )


@login_required
def financeiro_compra_detail(request, pk):
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    compra = get_object_or_404(
        Compra,
        Q(pk=pk),
        Q(caderno__criador=cliente) | Q(caderno__id_financeiro__in=cliente.financeiros.all()),
    )
    message = request.GET.get("msg", "").strip()
    message_level = request.GET.get("level", "").strip() or "info"
    open_cadastro = request.GET.get("cadastro", "").strip()
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_categoria":
            nome = request.POST.get("categoria_nome", "").strip()
            if not nome:
                msg = "Informe um nome de categoria."
                level = "error"
                created = False
            else:
                categoria, created = CategoriaCompra.objects.get_or_create(nome=nome)
                if created:
                    msg = "Categoria criada."
                    level = "success"
                else:
                    msg = "Categoria ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": categoria.id if nome and "categoria" in locals() else None,
                        "nome": categoria.nome if nome and "categoria" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            params = {"cadastro": "categoria", "msg": msg, "level": level}
            return redirect(f"{reverse('financeiro_compra_detail', kwargs={'pk': compra.pk})}?{urlencode(params)}")
        if action == "create_centro":
            nome = request.POST.get("centro_nome", "").strip()
            if not nome:
                msg = "Informe um nome de centro de custo."
                level = "error"
                created = False
            else:
                centro, created = CentroCusto.objects.get_or_create(nome=nome)
                if created:
                    msg = "Centro de custo criado."
                    level = "success"
                else:
                    msg = "Centro de custo ja existe."
                    level = "warning"
            if request.headers.get("x-requested-with") == "XMLHttpRequest":
                return JsonResponse(
                    {
                        "ok": bool(nome),
                        "created": created,
                        "id": centro.id if nome and "centro" in locals() else None,
                        "nome": centro.nome if nome and "centro" in locals() else None,
                        "message": msg,
                        "level": level,
                    }
                )
            params = {"cadastro": "centro", "msg": msg, "level": level}
            return redirect(f"{reverse('financeiro_compra_detail', kwargs={'pk': compra.pk})}?{urlencode(params)}")
        if action == "delete_compra":
            caderno_id = compra.caderno_id
            compra.delete()
            if caderno_id:
                return redirect("financeiro_caderno_detail", pk=caderno_id)
            return redirect("financeiro")
        if action == "update_compra":
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            categoria_id = request.POST.get("categoria")
            centro_id = request.POST.get("centro_custo")
            caderno_id = request.POST.get("caderno")
            data_raw = request.POST.get("data", "").strip()
            allowed_cadernos = Caderno.objects.filter(
                Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all())
            )
            if caderno_id and not allowed_cadernos.filter(id=caderno_id).exists():
                return redirect("financeiro_compra_detail", pk=compra.pk)
            if data_raw:
                try:
                    data = datetime.strptime(data_raw, "%Y-%m-%d").date()
                except ValueError:
                    data = None
            else:
                data = None
            compra.nome = nome
            compra.descricao = descricao
            compra.categoria_id = categoria_id or None
            compra.centro_custo_id = centro_id or None
            compra.data = data
            if caderno_id:
                compra.caderno_id = caderno_id
            compra.save(update_fields=["nome", "descricao", "categoria", "centro_custo", "caderno", "data"])
            return redirect("financeiro_compra_detail", pk=compra.pk)
        if action == "copy_next_months":
            meses_raw = request.POST.get("meses", "").strip()
            try:
                meses = int(meses_raw)
            except (TypeError, ValueError):
                meses = 0
            meses = max(0, min(24, meses))
            if meses <= 0:
                msg = "Informe a quantidade de meses para copiar."
                params = {"msg": msg, "level": "error"}
                return redirect(
                    f"{reverse('financeiro_compra_detail', kwargs={'pk': compra.pk})}?{urlencode(params)}"
                )
            if not compra.data:
                msg = "Defina uma data para copiar para os proximos meses."
                params = {"msg": msg, "level": "error"}
                return redirect(
                    f"{reverse('financeiro_compra_detail', kwargs={'pk': compra.pk})}?{urlencode(params)}"
                )
            itens_origem = list(compra.itens.all())
            for offset in range(1, meses + 1):
                target_date = _add_months(compra.data, offset)
                existing = Compra.objects.filter(
                    caderno_id=compra.caderno_id,
                    nome=compra.nome,
                    categoria_id=compra.categoria_id,
                    centro_custo_id=compra.centro_custo_id,
                    valor=compra.valor,
                    data=target_date,
                ).first()
                if existing:
                    existing.descricao = compra.descricao
                    existing.valor = compra.valor
                    existing.caderno_id = compra.caderno_id
                    existing.categoria_id = compra.categoria_id
                    existing.centro_custo_id = compra.centro_custo_id
                    existing.data = target_date
                    existing.save(
                        update_fields=[
                            "descricao",
                            "valor",
                            "caderno",
                            "categoria",
                            "centro_custo",
                            "data",
                        ]
                    )
                    existing.itens.all().delete()
                    alvo = existing
                else:
                    alvo = Compra.objects.create(
                        caderno_id=compra.caderno_id,
                        nome=compra.nome,
                        descricao=compra.descricao,
                        valor=compra.valor,
                        data=target_date,
                        categoria_id=compra.categoria_id,
                        centro_custo_id=compra.centro_custo_id,
                    )
                itens_novos = [
                    CompraItem(
                        compra=alvo,
                        nome=item.nome,
                        valor=item.valor,
                        quantidade=item.quantidade,
                        tipo_id=item.tipo_id,
                        pago=item.pago,
                    )
                    for item in itens_origem
                ]
                if itens_novos:
                    CompraItem.objects.bulk_create(itens_novos)
            msg = "Compra copiada para os proximos meses."
            params = {"msg": msg, "level": "success"}
            return redirect(
                f"{reverse('financeiro_compra_detail', kwargs={'pk': compra.pk})}?{urlencode(params)}"
            )
        if action == "add_item":
            nome = request.POST.get("nome", "").strip()
            valor_raw = request.POST.get("valor", "").replace(",", ".").strip()
            quantidade_raw = request.POST.get("quantidade", "").strip()
            tipo_id = request.POST.get("tipo")
            pago = request.POST.get("pago") == "on"
            try:
                valor = Decimal(valor_raw)
            except (InvalidOperation, ValueError):
                valor = None
            try:
                quantidade = int(quantidade_raw) if quantidade_raw else 1
            except ValueError:
                quantidade = 1
            quantidade = max(1, quantidade)
            if nome:
                CompraItem.objects.create(
                    compra=compra,
                    nome=nome,
                    valor=valor,
                    quantidade=quantidade,
                    tipo_id=tipo_id or None,
                    pago=pago,
                )
            return redirect("financeiro_compra_detail", pk=compra.pk)
        if action == "toggle_item_pago":
            item_id = request.POST.get("item_id")
            item = get_object_or_404(CompraItem, pk=item_id, compra=compra)
            item.pago = not item.pago
            item.save(update_fields=["pago"])
            return redirect("financeiro_compra_detail", pk=compra.pk)
        if action == "update_item":
            item_id = request.POST.get("item_id")
            item = get_object_or_404(CompraItem, pk=item_id, compra=compra)
            nome = request.POST.get("nome", "").strip()
            valor_raw = request.POST.get("valor", "").replace(",", ".").strip()
            quantidade_raw = request.POST.get("quantidade", "").strip()
            tipo_id = request.POST.get("tipo")
            pago = request.POST.get("pago") == "on"
            try:
                valor = Decimal(valor_raw)
            except (InvalidOperation, ValueError):
                valor = None
            try:
                quantidade = int(quantidade_raw) if quantidade_raw else item.quantidade
            except ValueError:
                quantidade = item.quantidade
            quantidade = max(1, quantidade)
            if nome:
                item.nome = nome
            item.valor = valor
            item.quantidade = quantidade
            item.tipo_id = tipo_id or None
            item.pago = pago
            item.save(update_fields=["nome", "valor", "quantidade", "tipo", "pago"])
            return redirect("financeiro_compra_detail", pk=compra.pk)
        if action == "delete_item":
            item_id = request.POST.get("item_id")
            item = get_object_or_404(CompraItem, pk=item_id, compra=compra)
            item.delete()
            return redirect("financeiro_compra_detail", pk=compra.pk)
    compra.status_label = _compra_status_label(compra)
    itens = list(compra.itens.select_related("tipo").order_by("id"))
    for item in itens:
        item.total_valor = (item.valor or 0) * (item.quantidade or 0)
    compra.total_itens = sum(item.total_valor for item in itens)
    tipos = TipoCompra.objects.order_by("nome")
    categorias = CategoriaCompra.objects.order_by("nome")
    centros = CentroCusto.objects.order_by("nome")
    cadernos = Caderno.objects.filter(Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all())).order_by("nome")
    return render(
        request,
        "core/financeiro_compra_detail.html",
        {
            "compra": compra,
            "itens": itens,
            "tipos": tipos,
            "categorias": categorias,
            "centros": centros,
            "cadernos": cadernos,
            "message": message,
            "message_level": message_level,
            "open_cadastro": open_cadastro,
        },
    )


@login_required
def admin_explorar(request):
    if not request.user.is_staff:
        return HttpResponseForbidden("Sem permissao.")
    cliente_id = request.GET.get("cliente_id")
    cliente_q = request.GET.get("cliente_q", "").strip()
    cliente_sort = request.GET.get("cliente_sort", "nome")
    proposta_status = request.GET.get("proposta_status", "").strip()
    proposta_sort = request.GET.get("proposta_sort", "-criado_em")

    clientes = PerfilUsuario.objects.all()
    if cliente_q:
        clientes = clientes.filter(nome__icontains=cliente_q)
    if cliente_sort == "empresa":
        clientes = clientes.order_by("empresa", "nome")
    elif cliente_sort == "email":
        clientes = clientes.order_by("email", "nome")
    else:
        clientes = clientes.order_by("nome")

    cliente = None
    propostas = Proposta.objects.none()
    if cliente_id:
        cliente = get_object_or_404(PerfilUsuario, pk=cliente_id)
        propostas = Proposta.objects.filter(cliente=cliente)
        if proposta_status == "pendente":
            propostas = propostas.filter(aprovada__isnull=True)
        elif proposta_status == "aprovada":
            propostas = propostas.filter(aprovada=True)
        elif proposta_status == "reprovada":
            propostas = propostas.filter(aprovada=False)
        elif proposta_status == "finalizada":
            propostas = propostas.filter(finalizada=True)
        if proposta_sort == "prioridade":
            propostas = propostas.order_by("prioridade", "-criado_em")
        elif proposta_sort == "valor":
            propostas = propostas.order_by("-valor", "-criado_em")
        else:
            propostas = propostas.order_by("-criado_em")
    return render(
        request,
        "admin/explorar.html",
        {
            "clientes": clientes,
            "cliente": cliente,
            "propostas": propostas,
            "cliente_q": cliente_q,
            "cliente_sort": cliente_sort,
            "proposta_status": proposta_status,
            "proposta_sort": proposta_sort,
        },
    )


@login_required
def ajustes_sistema(request):
    if not request.user.is_staff:
        return HttpResponseForbidden("Sem permissao.")
    message = None
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_channel_type":
            nome = request.POST.get("nome", "").strip().upper()
            if nome:
                TipoCanalIO.objects.get_or_create(nome=nome, defaults={"ativo": True})
    channel_types = TipoCanalIO.objects.filter(ativo=True).order_by("nome")
    return render(
        request,
        "core/ajustes.html",
        {
            "message": message,
            "channel_types": channel_types,
        },
    )
