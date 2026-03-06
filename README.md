# Stockage-et-structuration-des-donn-es-de-ventes-Superstore-avec-PostgreSQL-et-SQLAlchemy
# 📊 Rapport de Modélisation et d’Intégration des Données

## 🚀 Projet : Optimisation et Structuration du dataset "Superstore Sales"

**🛠️ Outils :** Python (SQLAlchemy, Pandas) & PostgreSQL
**🎯 Objectif :** Concevoir une base de données relationnelle fiable et charger les données nettoyées afin de permettre l’analyse décisionnelle.

---

# 1️⃣ 📝 Introduction

Après la phase de nettoyage et de transformation des données, l’objectif de cette étape est de structurer les données dans une base de données relationnelle PostgreSQL.

Le dataset nettoyé **superstore_clean.csv** contient des informations sur :

* les clients
* les produits
* les commandes
* les ventes
* les données géographiques

Afin d'éviter la redondance et améliorer l'intégrité des données, une modélisation relationnelle normalisée a été mise en place.

---

# 2️⃣ 🧩 Phase 1 : Conception du Modèle de Données

Pour organiser les données efficacement, un modèle relationnel a été conçu selon les principes de normalisation des bases de données.

**Tables principales créées :**

* Geography
* Customers
* Products
* Orders
* Order_Details

Chaque table représente une entité spécifique du système de vente.

---

# 3️⃣ 🗺️ Phase 2 : Modélisation Relationnelle (ERD)

Les relations entre les tables sont définies par des clés primaires et étrangères.

**Relations principales :**

* Geography **1 ─── N** Customers
* Customers **1 ─── N** Orders
* Orders **1 ─── N** Order_Details
* Products **1 ─── N** Order_Details

### Description :

📍 **Geography** contient les informations géographiques telles que la ville, l'état et la région.

👤 **Customers** stocke les informations des clients et leur localisation.

📦 **Products** contient les détails des produits vendus.

🧾 **Orders** représente les commandes effectuées par les clients.

📊 **Order_Details** stocke les informations financières des ventes.

---

# 4️⃣ 🏗️ Phase 3 : Création de la Base de Données

La base de données a été créée dans PostgreSQL en utilisant la bibliothèque **SQLAlchemy ORM**.

SQLAlchemy permet de :

* définir les tables en Python
* créer automatiquement les structures SQL
* gérer les relations entre les tables

**Exemple de définition d'une table :**

```
Products
---------
product_id (Primary Key)
product_name
category
sub_category
```

Des contraintes ont été ajoutées pour garantir l'intégrité des données.

**Exemple :**

```
CHECK (sales >= 0)
```

pour empêcher l'enregistrement de ventes négatives.

---

# 5️⃣ 📥 Phase 4 : Chargement des Données

Le fichier **superstore_clean.csv** a été importé avec **Pandas**, puis chargé dans la base de données.

### Étapes de chargement :

* Chargement des données géographiques
* Chargement des produits
* Chargement des clients
* Chargement des commandes
* Chargement des détails de commandes

La méthode suivante a été utilisée :

```
session.merge()
```

Cette méthode permet :

* d'insérer les nouvelles données
* d'éviter les doublons

---

# 6️⃣ 🛡️ Phase 5 : Validation de l’Intégrité des Données

Plusieurs tests ont été réalisés afin de vérifier la cohérence des données.

### 🔗 Test d'intégrité référentielle

Une tentative d'insertion d'un produit inexistant dans **Order_Details** a été effectuée.

**Résultat :**

```
Foreign Key Constraint Error
```

Ce test confirme que la base de données empêche l'insertion de données incohérentes.

### 💰 Vérification des ventes

Une comparaison a été réalisée entre :

* Total Sales dans le fichier CSV
* Total Sales dans la base de données

Les deux résultats étant identiques, cela confirme que les données ont été correctement importées.

---

# 7️⃣ 📊 Phase 6 : Analyse des Données

Plusieurs requêtes SQL ont été exécutées pour analyser les données.

### Exemple d'analyse

📍 **Total des ventes par région**

Cette requête permet d’identifier les régions générant le plus de chiffre d’affaires.

📦 **Produits les plus rentables**

Identification des **5 produits avec le profit le plus élevé**.

---

# 8️⃣ ⚡ Phase 7 : Optimisation des Performances

Pour améliorer les performances des requêtes, plusieurs **index** ont été créés.

**Exemples :**

* Index sur la date de commande
* Index sur la région
* Index sur la catégorie des produits

Ces index permettent d'accélérer considérablement les analyses sur de grands volumes de données.

---

# 9️⃣ 📊 Phase 8 : Création de Vues d’Analyse

Deux vues principales ont été créées pour faciliter l’analyse.

### View_Master_Sales

Cette vue regroupe les informations provenant de plusieurs tables :

* Orders
* Customers
* Products
* Geography
* Order_Details

Elle permet d'obtenir une **vue globale des ventes**.

### View_KPI_Summary

Cette vue calcule plusieurs indicateurs de performance (**KPIs**) :

* total des ventes
* total du profit
* nombre de commandes
* marge bénéficiaire

Ces indicateurs permettent une **analyse rapide de la performance commerciale**.

---

# 🔟 📥 Conclusion

Cette phase du projet a permis de transformer un simple fichier **CSV** en système de **base de données relationnelle structuré**.

Les principales réalisations sont :

* conception d’un modèle de données normalisé
* création des tables dans PostgreSQL
* chargement sécurisé des données
* validation de l'intégrité des données
* création de vues analytiques et d'indicateurs de performance

La base de données est désormais prête à être utilisée pour des outils de **Business Intelligence** tels que **Power BI ou Tableau**.
