# GDPR Use Case
L'idée est de développer 3 services :

1. Un service qui nous permet de supprimer des données depuis des fichiers CSV ou PARQUET (par exemple, nous pouvons avoir des données client, et un des clients voudrait supprimer les données qui lui sont relatives en nous fournissant son identifiant. Nous devrons être en mesure de le faire)
2. Pareil que pour le service 1., ce service va nous permettre de hasher les données d'un client lorsqu'il le demande.
3. Enfin, un 3ème service qui recevra en entrée l'identifiant d'un client, et qui devra générer un fichier CSV contenant toutes les données relatives à celui-ci, et les lui envoyer par email, par exemple.

