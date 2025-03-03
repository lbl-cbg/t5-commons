from django.urls import path

from . import views

urlpatterns = [
    path("species-list", views.get_species_list, name="species_list"),
    path("species/<str:taxonid>/target/<str:braveid>", views.get_target, name="target"),
    path("species/<str:taxonid>", views.get_species, name="species"),
]