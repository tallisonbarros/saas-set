from django import forms
from django.contrib.auth.models import Group, User

from .models import Cliente


class ClienteAdminForm(forms.ModelForm):
    senha_inicial = forms.CharField(
        label="Senha inicial",
        required=False,
        widget=forms.PasswordInput(render_value=True),
    )

    class Meta:
        model = Cliente
        fields = ["nome", "empresa", "sigla_cidade", "email", "logo", "ativo"]

    def clean_email(self):
        email = self.cleaned_data["email"].strip().lower()
        existing = User.objects.filter(username=email)
        if self.instance.pk:
            existing = existing.exclude(pk=self.instance.usuario_id)
        if existing.exists():
            raise forms.ValidationError("Email ja usado por outro usuario.")
        return email

    def save(self, commit=True):
        cliente = super().save(commit=False)
        senha_inicial = self.cleaned_data.get("senha_inicial")
        email = self.cleaned_data.get("email")

        if cliente.usuario_id:
            usuario = cliente.usuario
        else:
            usuario = User(username=email, email=email, is_active=True)
        usuario.username = email
        usuario.email = email
        if senha_inicial:
            usuario.set_password(senha_inicial)
        elif not usuario.pk:
            usuario.set_unusable_password()
        usuario.save()
        cliente.usuario = usuario
        if commit:
            cliente.save()
            self.save_m2m()
        return cliente


class UserCreateForm(forms.Form):
    username = forms.EmailField(label="Email")
    password = forms.CharField(label="Senha", widget=forms.PasswordInput)
    is_staff = forms.BooleanField(label="Administrador", required=False)
    groups = forms.ModelMultipleChoiceField(
        label="Grupos",
        queryset=Group.objects.all(),
        required=False,
        widget=forms.SelectMultiple,
    )

    def clean_username(self):
        username = self.cleaned_data["username"].strip().lower()
        if User.objects.filter(username=username).exists():
            raise forms.ValidationError("Email ja cadastrado.")
        return username

    def save(self):
        username = self.cleaned_data["username"]
        password = self.cleaned_data["password"]
        is_staff = self.cleaned_data["is_staff"]
        groups = self.cleaned_data.get("groups")
        user = User.objects.create_user(
            username=username,
            email=username,
            password=password,
        )
        user.is_staff = is_staff
        user.save(update_fields=["is_staff"])
        if groups:
            user.groups.set(groups)
        return user


class GroupCreateForm(forms.Form):
    name = forms.CharField(label="Nome do grupo", max_length=150)

    def clean_name(self):
        name = self.cleaned_data["name"].strip()
        if Group.objects.filter(name=name).exists():
            raise forms.ValidationError("Grupo ja existe.")
        return name

    def save(self):
        name = self.cleaned_data["name"]
        return Group.objects.create(name=name)
