# Labo 03 – REST APIs, GraphQL

<img src="https://upload.wikimedia.org/wikipedia/commons/2/2a/Ets_quebec_logo.png" width="250">    
ÉTS - LOG430 - Architecture logicielle - Chargé de laboratoire: Gabriel C. Ullmann, Automne 2025.

## 🎯 Objectifs d'apprentissage

- Comprendre ce qu'est une API REST et les principes RESTful.
- Comprendre comment une API peut contribuer à l'extensibilité d'une application et faciliter l'intégration de nouveaux clients.
- Apprendre comment utiliser GraphQL pour créer une API plus flexible offrant aux clients la possibilité de requêter exactement les données dont ils ont besoin.
- Comparer les avantages et inconvénients des approches REST et GraphQL selon différents contextes d'utilisation.
- Maîtriser la gestion d'état et de cache avec Redis dans un contexte d'API moderne.
- Comprendre l'utilisation des jointures SQL avec SQLAlchemy pour optimiser les requêtes de base de données.

## ⚙️ Setup 

Dans ce laboratoire, nous poursuivrons le développement de notre application de gestion de magasin. Dans l'aspect architectural, nous transformons maintenant notre application monolithique en API avec [Flask](https://www.geeksforgeeks.org/python/flask-tutorial/) pour lui donner plus de flexibilité. Dans l'aspect fonctionnel, nous ajoutons la gestion du stock des articles, en complément de la gestion des commandes, des articles et des utilisateurs. L'application comporte désormais deux domaines : les commandes et les stocks, qui sont clairement identifiés dans la structure de répertoires (`/src/orders` et `/src/stocks`). 

> ⚠️ IMPORTANT : Avant de commencer le setup et les activités, veuillez lire la documentation architecturale dans le répertoire `/docs/arc42/docs.pdf`.

### 1. Créez un nouveau dépôt à partir du gabarit et clonez le dépôt
```bash
git clone https://github.com/[votredepot]/log430-a25-labo3
cd log430-a25-labo3
```

### 2. Créez un réseau Docker
Éxecutez dans votre terminal:
```bash
docker network create labo03-network
```

### 3. Préparez l'environnement de développement
Suivez les mêmes étapes que dans le laboratoire 02. Ensuite, créez et lancez le conteneur Docker.
```bash
docker build
docker compose up -d
```

### 4. Installez Postman
[Installez Postman](https://learning.postman.com/docs/getting-started/installation/installation-and-updates/) et [importez la collection](https://www.geeksforgeeks.org/websites-apps/how-to-import-export-collections-in-postman/) disponible dans `/docs/collections`. 

### 5. Comprenez les principes REST
À ce stade, notre application est une API qui respecte presque tous les principes REST définis par Roy Fielding dans sa thèse de doctorat (2000) :

- ✅ **Client–Serveur** : séparation claire entre client et serveur.
- ✅ **Système en couches** : notre application comporte trois couches (front-end, back-end, base de données).
- ✅ **Sans état (stateless)** : chaque requête est indépendante, le serveur ne « se souvient » pas des requêtes précédentes.
- ⛔ **Cache** : il n'y a pas de mécanisme de cache côté client (nous utilisons Postman, mais cela serait possible avec un front-end).
- ✅ **Interface uniforme** : les endpoints sont bien nommés et utilisent les bonnes méthodes HTTP (POST /orders, GET /products/:id, etc.).

Une API qui respecte l'ensemble de ces principes est appelée une API RESTful. Pour l'instant, nous travaillons uniquement avec une API REST.

## 🧪 Activités pratiques

### 1. Testez le processus de stock complet

Dans `src/tests/test_store_manager.py`, dans la méthode `test_stock_flow()`, écrivez un [smoke test](https://www.techtarget.com/searchsoftwarequality/definition/smoke-testing) pour que nous puissions observer comment le processus de stock fonctionne, et aussi nous assurer qu'il fonctionne de manière consistante. Testez les endpoints suivants :

1. Créez un article (`POST /products`)
2. Ajoutez 5 unités au stock de cet article (`POST /stocks`)
3. Vérifiez le stock, votre article devra avoir 5 unités dans le stock (`GET /stocks/:id`)
4. Faites une commande de 2 unités de l'article que vous avez créé  (`POST /orders`)
5. Vérifiez le stock encore une fois (`GET /stocks/:id`)
6. **Étape extra**: supprimez la commande et vérifiez le stock de nouveau. Le stock devrait augmenter après la suppression de la commande.

Exécutez vos tests pour vous assurer que le flux de stock fonctionne correctement.

> 💡 **Question 1** : Quel nombre d'unités de stock pour votre article avez-vous obtenu à la fin du test ? Et pour l'article avec `id=2` ? Veuillez inclure la sortie de votre Postman pour illustrer votre réponse.

### 2. Créez un rapport de stock

Le directeur du magasin qui utilise notre application a besoin de connaître l'état des articles dans le stock. Dans `src/queries/read_stock.py`, il y a une méthode `get_stock_for_all_products`, qui est utilisée par l'endpoint `/stocks/reports/overview-stocks` pour donner les stocks de chaque article, mais il n'y a pas beaucoup d'informations. Ajoutez les colonnes `name`, `sku` et `price` de l'article en utilisant la méthode [join à SQLAlchemy](https://docs.sqlalchemy.org/en/14/orm/query.html#sqlalchemy.orm.Query.join). Cela vous permettra de joindre l'information du tableau `Stock` avec `Product`.

> 💡 **Question 2** : Décrivez l'utilisation de la méthode join dans ce cas. Utilisez les méthodes telles que décrites à `Simple Relationship Joins` et `Joins to a Target with an ON Clause` dans la documentation SQLAlchemy pour ajouter les colonnes demandées dans cette activité. Veuillez inclure le code pour illustrer votre réponse.

### 3. Utilisez l'endpoint GraphQL

Dans l'activité 2, nous avons ajouté de nouvelles colonnes `Product` à un endpoint `Stock`. Si à l'avenir nous avons de nouvelles colonnes dans `Product` ou `Stock`, ou le besoin de conserver différents endpoints avec des colonnes distinctes, il faudra que nous créions différents endpoints. Pour nous aider à mieux gérer l'hétérogénéité des endpoints, on peut créer un endpoint GraphQL.

GraphQL est un langage qui nous permet de donner la possibilité aux clients qui utilisent notre API REST de continuer à utiliser les endpoints avec les noms et méthodes fixés, mais en passant les noms des colonnes qu'ils veulent. Par exemple :

```graphql
{
  product(id: "1") {
    id
    quantity
  }
}
```

L'endpoint GraphQL est accessible via `POST /stocks/graphql-query`.

> 💡 **Question 3** : Quels résultats avez-vous obtenus en utilisant l’endpoint `POST /stocks/graphql-query` avec la requête suggérée ? Veuillez joindre la sortie de votre requête dans Postman afin d’illustrer votre réponse.

### 4. Ajoutez plus d'informations à l'endpoint GraphQL

La correspondance entre les colonnes GraphQL et les données est définie dans `/schemas/query.py`, au sein de la méthode `resolve_product`. Ajoutez également les colonnes `name`, `sku` et `price` afin que les clients puissent les interroger via GraphQL. Adaptez aussi la méthode `update_stock_redis` (fichier `src/commands/write_stock.py`) afin d’enregistrer davantage d’informations manquantes sur l’article dans Redis.

> 💡 **Question 4** : Quelles lignes avez-vous changé dans `update_stock_redis`? Veuillez joindre du code afin d’illustrer votre réponse.

> 💡 **Question 5** : Quels résultats avez-vous obtenus en utilisant l’endpoint `POST /stocks/graphql-query` avec les améliorations ? Veuillez joindre la sortie de votre requête dans Postman afin d’illustrer votre réponse.

### 5. Créez un autre conteneur pour effectuer un test de communication
Pour simuler un scénario plus proche de la réalité, exécutez `scripts/supplier_app.py` dans un conteneur séparé (comme si c'était sur le serveur de notre fournisseur). Observez les résultats. Si vous avez besoin de précisions supplémentaires, référez-vous au diagramme `docs/views/deployment.puml`. Vous pouvez vous appuyer sur les `Dockerfile` et le `docker-compose.yml` déjà présents dans le répertoire `scripts`.

**Extra**: modifiez le code GraphQL dans la variable `TEST_PAYLOAD` dans `scripts/supplier_app.py` pour inclure les colonnes `name`, `sku` et `price` de l'activité 4.

> 💡 **Question 6** : Examinez attentivement le fichier `docker-compose.yml` du répertoire `scripts`, ainsi que celui situé à la racine du projet. Qu’ont-ils en commun ? Par quel mécanisme ces conteneurs peuvent-ils communiquer entre eux ? Veuillez joindre du code YML afin d’illustrer votre réponse.

## Conseils de débogage
Si vous rencontrez des difficultés dans la réalisation des activités et que vous souhaitez voir plus en détail ce qui se passe dans les bases de données, vous pouvez utiliser [MySQL Workbench](https://www.mysql.com/products/community/) et [redis-cli](https://redis.io/docs/latest/operate/oss_and_stack/install/install-stack/docker/#connect-with-redis-cli) pour vérifier si les enregistrements sont correctement ajoutés/supprimés. Cependant, l'installation de ces logiciels n'est pas obligatoire pour la réalisation des activités.

## 📦 Livrables

- Un fichier .zip contenant l'intégralité du code source du projet Labo 03.
- Un rapport en .pdf répondant aux questions présentées dans ce document. Il est obligatoire d'illustrer vos réponses avec du code ou des captures d'écran/terminal.
