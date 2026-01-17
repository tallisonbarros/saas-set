from datetime import datetime
from decimal import Decimal, InvalidOperation

from django.contrib.auth import logout
from django.contrib.auth.decorators import login_required
from django.http import HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_POST

from django.contrib.auth.models import User
from django.db.models import Case, Count, DecimalField, F, IntegerField, OuterRef, Q, Subquery, Sum, Value, When
from django.db.models.expressions import ExpressionWrapper

from .forms import TipoPerfilCreateForm, UserCreateForm
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
    PlantaIO,
    Proposta,
    RackIO,
    RackSlotIO,
    TipoCompra,
    TipoCanalIO,
    TipoPerfil,
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


def home(request):
    if request.user.is_authenticated:
        logout(request)
    return render(request, "core/home.html")


@login_required
def painel(request):
    return render(
        request,
        "core/painel.html",
        {
            "role": _user_role(request.user),
            "is_financeiro": _has_tipo(request.user, "Financeiro") or request.user.is_staff,
            "is_cliente": _has_tipo_any(request.user, ["Contratante", "Cliente"]),
            "is_vendedor": _has_tipo(request.user, "Vendedor"),
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
            "prev_slot": prev_slot,
            "next_slot": next_slot,
        },
    )


@login_required
def proposta_list(request):
    if _user_role(request.user) == "FINANCEIRO":
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    propostas = Proposta.objects.none()
    if _has_tipo(request.user, "Vendedor"):
        if cliente:
            propostas = (
                Proposta.objects.filter(Q(criada_por=request.user) | Q(cliente=cliente))
                .distinct()
                .order_by("-criado_em")
            )
        else:
            propostas = Proposta.objects.filter(criada_por=request.user).order_by("-criado_em")
    elif cliente:
        propostas = Proposta.objects.filter(cliente=cliente).order_by("-criado_em")
    status = request.GET.get("status")
    if status == "pendente":
        propostas = propostas.filter(aprovada__isnull=True)
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
            When(finalizada=True, then=Value(4)),
            When(aprovada=True, then=Value(2)),
            When(aprovada=False, then=Value(3)),
            default=Value(1),
            output_field=IntegerField(),
        )
    ).order_by("status_order", "-criado_em")
    propostas = propostas.annotate(
        status_label=Case(
            When(aprovada=True, then=Value("Aprovada")),
            When(aprovada=False, then=Value("Reprovada")),
            default=Value("Pendente"),
        )
    )
    return render(
        request,
        "core/proposta_list.html",
        {
            "cliente": cliente,
            "propostas": propostas,
            "status_filter": status,
            "is_vendedor": _has_tipo(request.user, "Vendedor"),
            "current_user_id": request.user.id,
        },
    )


@login_required
def proposta_detail(request, pk):
    if _user_role(request.user) == "FINANCEIRO":
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    if _has_tipo(request.user, "Vendedor") and cliente:
        proposta_qs = Proposta.objects.filter(Q(criada_por=request.user) | Q(cliente=cliente))
        proposta = get_object_or_404(proposta_qs, pk=pk)
    else:
        proposta = get_object_or_404(Proposta, pk=pk, cliente=cliente)
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
        if action == "set_finalizada":
            if proposta.criada_por_id != request.user.id:
                return HttpResponseForbidden("Sem permissao.")
            if proposta.aprovada is None:
                message = "Aguardando aprovacao. Finalizacao so e possivel apos aprovacao."
            else:
                proposta.finalizada = True
                proposta.save(update_fields=["finalizada"])
                return redirect("proposta_detail", pk=proposta.pk)
        if action == "delete_proposta":
            if proposta.criada_por_id != request.user.id:
                return HttpResponseForbidden("Sem permissao.")
            if proposta.aprovada is not None:
                message = "Nao e possivel excluir apos aprovacao."
            else:
                proposta.delete()
                return redirect("propostas")
    return render(
        request,
        "core/proposta_detail.html",
        {
            "cliente": cliente,
            "proposta": proposta,
            "is_contratante": _has_tipo(request.user, "Contratante"),
            "message": message,
        },
    )


@login_required
def proposta_nova_vendedor(request):
    if not (_has_tipo(request.user, "Vendedor") or request.user.is_staff):
        return HttpResponseForbidden("Sem permissao.")
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
    if _user_role(request.user) == "FINANCEIRO":
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    proposta = get_object_or_404(Proposta, pk=pk, cliente=cliente)
    if proposta.cliente.usuario_id != request.user.id:
        return HttpResponseForbidden("Somente o destinatario pode aprovar.")
    if proposta.aprovada is None:
        proposta.aprovada = True
        proposta.decidido_em = timezone.now()
        proposta.aprovado_por = request.user
        proposta.save(update_fields=["aprovada", "decidido_em", "aprovado_por"])
    return redirect("propostas")


@login_required
@require_POST
def reprovar_proposta(request, pk):
    if _user_role(request.user) == "FINANCEIRO":
        return HttpResponseForbidden("Sem permissao.")
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
    if _user_role(request.user) == "FINANCEIRO":
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    if _has_tipo(request.user, "Vendedor") and cliente:
        proposta_qs = Proposta.objects.filter(Q(criada_por=request.user) | Q(cliente=cliente))
        proposta = get_object_or_404(proposta_qs, pk=pk)
    else:
        proposta = get_object_or_404(Proposta, pk=pk, cliente=cliente)
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
    if not (request.user.is_staff or _has_tipo(request.user, "Financeiro")):
        return HttpResponseForbidden("Sem permissao.")
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
    item_expr = ExpressionWrapper(
        F("itens__valor") * F("itens__quantidade"),
        output_field=DecimalField(max_digits=12, decimal_places=2),
    )
    cadernos = cadernos.annotate(total=Sum(total_expr)).order_by("nome")
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
    if not (request.user.is_staff or _has_tipo(request.user, "Financeiro")):
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")

    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_compra":
            if not cliente:
                return HttpResponseForbidden("Sem cadastro de cliente.")
            caderno_id = request.POST.get("caderno")
            nome = request.POST.get("nome", "").strip()
            descricao = request.POST.get("descricao", "").strip()
            data_raw = request.POST.get("data", "").strip()
            categoria_id = request.POST.get("categoria")
            centro_id = request.POST.get("centro_custo")
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
                ]
            )
            if has_any:
                if caderno_id and not allowed_cadernos.filter(id=caderno_id).exists():
                    return redirect("financeiro")
                Compra.objects.create(
                    caderno_id=caderno_id or None,
                    nome=nome,
                    descricao=descricao,
                    data=data,
                    categoria_id=categoria_id or None,
                    centro_custo_id=centro_id or None,
                )
            return redirect("financeiro")

    if cliente:
        cadernos = Caderno.objects.filter(Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all()))
    else:
        cadernos = Caderno.objects.none()
    categorias = CategoriaCompra.objects.order_by("nome")
    centros = CentroCusto.objects.order_by("nome")
    selected_caderno_id = request.GET.get("caderno_id") or ""

    return render(
        request,
        "core/financeiro_nova.html",
        {
            "cliente": cliente,
            "cadernos": cadernos,
            "categorias": categorias,
            "centros": centros,
            "selected_caderno_id": str(selected_caderno_id),
        },
    )


@login_required
def financeiro_cadernos(request):
    if not (request.user.is_staff or _has_tipo(request.user, "Financeiro")):
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")

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
        {"cadernos": cadernos},
    )


@login_required
def financeiro_caderno_detail(request, pk):
    if not (request.user.is_staff or _has_tipo(request.user, "Financeiro")):
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    caderno = get_object_or_404(
        Caderno,
        Q(pk=pk),
        Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all()),
    )
    compras = Compra.objects.filter(caderno=caderno).prefetch_related("itens").order_by("-data")
    for compra in compras:
        compra.status_label = _compra_status_label(compra)
        compra.total_itens = sum(
            (item.valor or 0) * (item.quantidade or 0) for item in compra.itens.all()
        )
    return render(
        request,
        "core/financeiro_caderno_detail.html",
        {"caderno": caderno, "compras": compras},
    )


@login_required
def financeiro_compra_detail(request, pk):
    if not (request.user.is_staff or _has_tipo(request.user, "Financeiro")):
        return HttpResponseForbidden("Sem permissao.")
    cliente = _get_cliente(request.user)
    if not cliente and not request.user.is_staff:
        return HttpResponseForbidden("Sem cadastro de cliente.")
    compra = get_object_or_404(
        Compra,
        Q(pk=pk),
        Q(caderno__criador=cliente) | Q(caderno__id_financeiro__in=cliente.financeiros.all()),
    )
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "delete_compra":
            caderno_id = compra.caderno_id
            compra.delete()
            if caderno_id:
                return redirect("financeiro_caderno_detail", pk=caderno_id)
            return redirect("financeiro")
        if action == "update_compra":
            nome = request.POST.get("nome", "").strip()
            categoria_id = request.POST.get("categoria")
            centro_id = request.POST.get("centro_custo")
            caderno_id = request.POST.get("caderno")
            allowed_cadernos = Caderno.objects.filter(
                Q(criador=cliente) | Q(id_financeiro__in=cliente.financeiros.all())
            )
            if caderno_id and not allowed_cadernos.filter(id=caderno_id).exists():
                return redirect("financeiro_compra_detail", pk=compra.pk)
            compra.nome = nome
            compra.categoria_id = categoria_id or None
            compra.centro_custo_id = centro_id or None
            if caderno_id:
                compra.caderno_id = caderno_id
            compra.save(update_fields=["nome", "categoria", "centro_custo", "caderno"])
            return redirect("financeiro_compra_detail", pk=compra.pk)
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
