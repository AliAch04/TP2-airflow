# Importer les bibliothèques d'Airflow nécessaires pour créer un DAG et les opérateurs 
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from airflow.utils.dates import days_ago 
# Importer des bibliothèques standards de Python pour la gestion des dates et les requêtes HTTP 
from datetime import datetime 
import requests 

# Fonction pour imprimer un message de bienvenue 
def print_welcome(): 
    print('Bienvenue sur Airflow!')
# Fonction pour imprimer la date actuelle 
def print_date(): 
    print(f"Aujourd'hui, nous sommes le {datetime.today().date()}")
# Fonction pour imprimer une citation aléatoire 
def print_random_cat_fact(): 
    response = requests.get('https://catfact.ninja/fact') 
    fact = response.json()['fact'] 
    print(f"Fait aléatoire sur les chats : \"{fact}\"") 

# Définition du DAG avec ses paramètres 
dag = DAG( 
    'welcome_dag',  # Nom unique du DAG 
    default_args={'start_date': days_ago(1)},  # Date de début : hier 
    schedule_interval='0 23 * * *',  # Exécution quotidienne à 23 heures 
    catchup=False  # Désactive l'exécution des tâches manquées 
)

print_welcome_task = PythonOperator( 
    task_id='print_welcome',  # Identifiant unique pour la tâche 
    python_callable=print_welcome,  # Fonction à appeler 
    dag=dag  # Le DAG auquel la tâche appartient 
)
print_date_task = PythonOperator( 
    task_id='print_date', 
    python_callable=print_date, 
    dag=dag 
)
print_random_quote_task = PythonOperator( 
task_id='print_random_quote', 
python_callable=print_random_cat_fact, 
dag=dag 
)

# Définir les dépendances entre les tâches 
print_welcome_task >> print_date_task >> print_random_quote_task 