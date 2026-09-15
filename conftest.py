"""Fichier de configuration lu automatiquement par pytest.

Sa seule présence à la racine du dépôt suffit : pytest ajoute le répertoire de
chaque `conftest.py` au `sys.path`, ce qui rend `import main` possible depuis
`tests/`. Sans ce fichier, les tests ne trouveraient pas les modules à tester,
qui vivent à la racine et non dans un paquet installable.

Il sert aussi à héberger les fixtures partagées par plusieurs fichiers de test.
"""

import pytest


@pytest.fixture
def profil_randomuser():
    """Un profil tel que randomuser.me le renvoie, réduit aux champs utilisés.

    C'est un *double de test* : il remplace un appel réseau réel. Les tests
    deviennent ainsi déterministes et exécutables hors ligne — condition pour
    qu'ils tournent en intégration continue.

    La région et le pays sont volontairement britanniques : c'est exactement ce
    que renvoie l'API (`nat=gb`), et c'est ce que le code doit réécrire.
    """
    return {
        "login": {"uuid": "11111111-2222-3333-4444-555555555555"},
        "name": {"first": "Awa", "last": "Ndiaye"},
        "dob": {"date": "1990-04-12T00:00:00.000Z"},
        "gender": "female",
        "nat": "GB",
        "id": {"value": "NN 12 34 56 A"},
        "location": {
            "street": {"number": 42, "name": "Baker Street"},
            "city": "Manchester",
            "state": "Greater Manchester",
            "country": "United Kingdom",
            "postcode": "M1 2AB",
        },
        "email": "awa.ndiaye@example.com",
        "phone": "015242 12345",
        "picture": {"large": "https://randomuser.me/api/portraits/women/1.jpg"},
        "registered": {"age": 7},
    }
